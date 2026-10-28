# 设计说明（v0.0.1-beta）

## 关键决定：不做通道，做全局连接器

Octop 的 `kind: channel` 扩展点用于“新增一个 IM 连接”。GsCore 不是 IM：它是下游业务核心，需要看到**已有通道**的消息，并把回复送回**同一个通道**。

因此本插件：

- **不注册 channel kind**，不创建数据库中的 channel 记录；
- **不拥有任何 IM 连接**；
- 只安装一个全局入站 tap，复用 Octop 现有通道的收发能力；
- 不改变 Octop Agent 的消息处理逻辑。

Octop 当前插件类型只有 `tool / skill / hook / channel`，没有 `connector`。本插件使用 `kind: hook` 作为全局生命周期挂载点；它不注册 middleware，也不出现在通道目录。

## 入站旁路点

`BaseChannel.handle_inbound` 是**所有通道**把平台事件送入 Agent 前的统一入口：

- 内置 QQ / 飞书通道在线程/回调里直接调用它；
- OneBot 11 插件通过 `ChannelManager.enqueue` 排队，worker 最终也调用它。

连接器在类级别包装一次 `BaseChannel.handle_inbound`：

```python
async def tapped(channel, raw_payload):
    connector.schedule_observe(channel, raw_payload)  # 异步旁路
    return await original(channel, raw_payload)        # Agent 原流程不变
```

为什么不在 `GlobalProcessor.__call__` 上挂：

- `GlobalProcessor` 只看到通过通道 `GroupContextManager` 门控后的消息；
- OneBot 11 的 `group_reply_mode=mention` 会在 Agent 侧丢弃未 @ 的群消息；
- 用户要求 GsCore “只要能接收到就一直监听”，所以必须在门控之前旁路。

副作用：连接器解析一次 `InboundMessage`，通道随后再解析一次。各内置/OneBot 的 `parse_inbound` 都是纯转换，不产生网络副作用；解析失败只记录日志，绝不影响原流程。

## 通道路由

每个上报包：

- `bot_id`：平台 ID（`onebot` / `qqgroup` / `qqguild` / `feishu` / 其他通道类型）；
- `bot_self_id`：**Octop 通道 ID**。它稳定、唯一，同时用于：
  - core 判定 `is_tome`（上报时插入 `at=bot_self_id`）；
  - core 回复时定位原通道实例。

`MessageSend.bot_self_id` 不在活跃通道表里时，按 `bot_id` 匹配候选通道；只有一个候选时兜底。

## 下发

- **OneBot 11**：利用 onebot11 插件的 `push_segments` / `push_forward`，支持 `at` / `reply` / 原生合并转发 / 断线 outbox；撤回、禁言直接调用其 action API。
- **QQ 官方 / 飞书**：复用内置通道的 `_send_content`，并绕过 `push_message`（它会剥离被动回复所需的 `msg_id`）。
- **其他通道**：走通用 `ContentPart` 下发。
- `echo` 非空时无论成功与否都回 `recall_message_id`。

## 配置与状态

- 配置放在 `$OCTOP_HOME/gscore/config.json`，不随插件目录重装丢失；
- 环境变量可覆盖；
- 每 2 秒检查 mtime，变更后重建 GsCoreClient（无需重启 Octop）；
- `$OCTOP_HOME/gscore/status.json` 输出连接状态、已观察通道、最近消息时间；
- `tools/configure.py` 写入配置，`tools/status.py` 读取状态。

## 生命周期

- `setup()` 安装 tap、设置全局 active connector、启动异步任务；
- 插件重载时新实例替换 `BaseChannel._gscore_connector`，旧实例被 stop；
- `OctopServer.stop` 被包装一次，服务器退出时优雅关闭 WebSocket；
- 插件禁用/卸载后建议重启 Octop（Octop 没有插件 teardown 回调）。

## 边界

- 通道在调用 `handle_inbound` 前主动丢弃的消息看不到（OneBot `group_reply_mode/private_reply_mode=off`）。
- QQ 官方 / 飞书没有 OneBot notice 语义，不伪造 meta 事件。
- `markdown` / `buttons` 等富交互段 warning 后跳过。
