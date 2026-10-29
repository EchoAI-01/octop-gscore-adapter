# Octop GsCore Connector

**v0.0.1-beta**

把 **Octop** 与 **GsCore / 早柚核心（gsuid-core）** 连接起来的全局连接器插件。

> 这不是一个 IM 通道。插件不会创建、占用或托管任何聊天连接。Octop 现有的 OneBot 11、QQ 官方、飞书等通道照常接收消息并交给 Agent；连接器只在消息进入 Agent 之前做一次旁路监听，把消息同步给 GsCore。GsCore 按自己的关键词/触发器规则决定是否回复，回复再通过**原通道**发回用户。

## 为什么是连接器而不是通道

```text
QQ / 飞书 / NapCat
        │ 平台原生协议
        ▼
┌─────────────────────────────┐
│ Octop 现有 IM 通道            │  OneBot11 / QQ / Feishu / …
│ （插件不创建、不修改）         │
└──────────────┬──────────────┘
               │ InboundMessage
               ▼
┌─────────────────────────────┐        ┌──────────────────┐
│ GsCore Connector（本插件）    │ ─────► │ GsCore / gsuid-core│
│ · 在群聊 Agent 门控之前旁路    │ ◄───── │ 关键词/触发器/插件 │
│ · 原样保留 Agent 流程          │        └──────────────────┘
└──────────────┬──────────────┘
               │ 原有处理流程
               ▼
        Octop Agent（LLM）
```

- **消息仍然进入 Octop Agent**：连接器不拦截、不修改、不阻断原通道。
- **GsCore 持续监听**：即使群消息没有 @ 机器人、原本会被通道的 Agent 门控过滤，连接器也能在门控之前看到并上报。
- **关键词触发由 GsCore 负责**：连接器不做关键词判断，只负责双向协议转换。
- **回复走原通道**：GsCore 的 `MessageSend` 通过上报时使用的那个通道实例发回，不需要重新配置 IM。

## 功能

| 方向 | 能力 |
|---|---|
| 上报 | 所有启用通道的入站消息：text / image / audio / video / file、at、reply + reply_id、node 合并转发、昵称/头像、`user_pm` |
| 下发 | text / image（`base64://`、`link://`、`file://`）/ record / video / file / at / reply / node |
| OneBot 增强 | `get_forward_msg` 展开、`get_msg` 引用正文、原生 `at`/`reply`/合并转发发送 |
| 元事件 | OneBot 11 `user_join_group` / `user_exit_group` / `poke` |
| 回执 | `MessageSend.echo` → `recall_message_id` |
| 控制包 | `excute_delete_message`、`excute_ban_user`（OneBot / 飞书按能力支持） |
| 生命周期 | GsCore 断线重连、配置热加载、状态文件、优雅停止 |

## 安装

前置：Octop 1.0.2（`octop-gateway 1.0.3` / `octop-harness 1.0.4`）已验证；Python 3.12；`websockets` 随 Octop 提供。

### Docker

```bash
# 把本目录放到 Octop 数据卷的插件目录
cp -a . /path/to/octop-data/plugins/octop-gscore-adapter
docker restart octop
```

启动日志应出现：

```text
[GsCoreConnector] plugin octop-gscore-adapter v0.0.1-beta installed
[GsCoreConnector] OneBot 11 notice hook installed
```

插件列表中的 `kind` 为 `hook`（Octop 当前没有可扩展的 `connector` 插件类型），但**不会**出现在通道目录里，也不会注册任何 `channel kind`。

### 本地插件安装

```bash
octop plugin install /path/to/octop-gscore-adapter --force
docker restart octop
```

## 配置

连接器没有 Octop 通道表单，配置独立存放在数据目录，不会随插件重装丢失：

```text
$OCTOP_HOME/gscore/config.json
# Docker 默认: /data/.octop/gscore/config.json
# 非 Docker 默认: ~/.octop/gscore/config.json
```

首次加载会自动创建一份 `enabled: false` 的默认配置。

### 在 Octop 页面中配置（推荐，无需 CLI）

Octop 自带的 **设置 → 环境变量** 页面可以直接填写以下变量，连接器会在约 2 秒内热加载：

```text
GSCORE_ENABLED=true
GSCORE_HOST=gsuid_core
GSCORE_PORT=8765
GSCORE_WS_TOKEN=你的 WS_TOKEN
GSCORE_BOT_ID=AstrBot
```

可选变量：

```text
GSCORE_ONLY_PREFIXES=core,gs
GSCORE_CHANNEL_TYPES=          # 留空=所有非控制通道
GSCORE_ONEBOT_BOT_ID=onebot
GSCORE_QQ_GROUP_BOT_ID=qq_official
GSCORE_FEISHU_BOT_ID=lark
GSCORE_ADMIN_USER_IDS=
```

> 为什么连接器列表里没有「GsCore」卡片：Octop 的 Connectors 是 **MCP 工具连接器目录**，`kind` 由核心 `catalog.json` 固定，插件只能注册 `tool / skill / hook / channel`，不能注册 connector kind。因此本插件使用 `kind: hook`，配置走 Octop 的「环境变量」页面或配置文件；这两种方式都不需要改 Octop 核心。若必须在 Connectors 页面出现专用卡片，则需要对 Octop 核心做扩展，而不是插件本身能完成的事情。

### 使用配置工具

```bash
docker exec octop python /data/.octop/plugins/octop-gscore-adapter/tools/configure.py \
  --host gsuid_core \
  --port 8765 \
  --bot-id Octop \
  --channel-types onebot11,qq,feishu \
  --enable
```

- Token 建议通过 stdin 传入，避免 shell 历史泄露：`--token-stdin`。
- 修改后连接器会在约 2 秒内热加载，无需重启 Octop。
- 查看脱敏后的有效配置：`configure.py --show`。
- 查看运行状态：`docker exec octop python /data/.octop/plugins/octop-gscore-adapter/tools/status.py`。

### 配置字段

| 字段 | 默认 | 说明 |
|---|---|---|
| `enabled` | `false` | 是否启动 GsCore 连接 |
| `gscore_host` | `localhost` | GsCore 主机 / Docker 服务名 |
| `gscore_port` | `8765` | GsCore 端口 |
| `gscore_ws_url` | 空 | 完整地址（高级）；支持 `{bot_id}`；留空自动拼 `/ws/{bot_id}` |
| `gscore_ws_token` | 空 | 对应 core 的 `WS_TOKEN` |
| `gscore_bot_id` | `AstrBot` | 连接级路由 ID，与参考项目 `BOT_ID` 默认值一致 |
| `gscore_max_retry` | `30` | 连续失败上限；`-1` 无限重试 |
| `gscore_reconnect_seconds` | `5` | 断线重连间隔 |
| `gscore_connect_timeout` | `8` | 建连超时 |
| `onebot_bot_id` | `onebot` | 上报给 GsCore 的 OneBot 平台 ID |
| `qq_group_bot_id` | `qq_official` | QQ 官方平台 ID（参考项目口径，群/单聊/频道统一） |
| `feishu_bot_id` | `lark` | 飞书平台 ID（参考项目口径） |
| `gscore_only_prefixes` | 空 | 命中前缀的消息仍上报 GsCore，但跳过 Octop Agent（等价 `event.stop_event()`） |
| `channel_types` | 空 | 要旁路监听的通道类型；**空表示所有非控制通道** |
| `admin_user_ids` | 空 | 上报 `user_pm=1` 的平台用户 ID，逗号分隔 |
| `status_interval_seconds` | `2` | 状态文件/配置热加载轮询间隔 |

`user_pm` 与参考项目一致：群主/管理员或 `admin_user_ids` 命中为 `1`，其余为 `6`。

环境变量可覆盖同名配置：`GSCORE_ENABLED`、`GSCORE_HOST`、`GSCORE_PORT`、`GSCORE_WS_TOKEN`、`GSCORE_BOT_ID`、`GSCORE_ONLY_PREFIXES`、`GSCORE_CHANNEL_TYPES` 等。

同时兼容参考项目的原始键名：`BOT_ID`、`IP`、`PORT`、`WS_TOKEN`、`MAX_RETRY_TIMES`、`GSCORE_ONLY_PREFIXES`。

## 通道侧注意事项

连接器依赖 Octop 现有通道把消息送进 `BaseChannel.handle_inbound`。以下情况消息不会到达连接器：

- OneBot 11 插件配置 `group_reply_mode: off`（群消息在插件内被直接丢弃）。
- OneBot 11 插件配置 `private_reply_mode: off`。
- 通道自身未连接/未登录。

若希望 GsCore 持续监听群聊，请保持 OneBot 11 通道的 `group_reply_mode` 为 `mention` 或 `always`；`mention` 不会影响连接器，因为连接器在 Agent 门控之前执行。

## 协议映射

- **平台 ID（与参考项目一致）**：OneBot 11 → `onebot`；QQ 官方（群/单聊/频道）→ `qq_official`；飞书 → `lark`；其他通道 → 通道类型名。
- **账号 ID**：`bot_self_id` 使用 Octop 通道 ID，对应参考项目的 `event.get_platform_id()`。`@` 机器人时上报 `at=<通道 ID>`，core 据此设置 `is_tome`。
- **消息 ID**：`MessageReceive.msg_id` 与参考项目一致，使用平台会话 ID；平台原始 `message_id` 缓存在连接器内，用于 QQ 被动回复、引用和回执。
- **引用**：上报 `reply_id`（平台消息 ID）+ `reply`（正文）；下发 `reply` / `reply_id` 都按消息 ID 引用。
- **合并转发**：上报展开为扁平 `node`；OneBot 下发使用原生合并转发，其他通道逐条展开。
- **回执**：`echo` 非空一定回 `recall_message_id`；拿不到平台 ID 也回 `id=None`。
- **控制包**：`excute_delete_message` / `excute_ban_user` 只执行控制动作，绝不当普通消息发送。

## 测试

在 Octop 容器内运行：

```bash
cd /path/to/octop-gscore-adapter

python tools/test_offline.py       # 配置、序列化、内容转换、hook 幂等
python tools/test_connector.py     # mock GsCore + fake 通道：旁路/Agent并存、回复、回执、控制包
# 真实 Octop 进程验证见下方 live_verify.py
```

真实 Octop 进程验证（mock GsCore + mock NapCat + 现有 OneBot 通道）：

```bash
# 1. 启用连接器并指向 mock core
docker exec octop python /data/.octop/plugins/octop-gscore-adapter/tools/configure.py \
  --host 172.17.0.1 --port 18765 --channel-types onebot11 --enable

# 2. 启动 mock GsCore + mock NapCat（独立容器，映射 18765/13001）
docker run -d --name gscore-connector-mocks \
  -p 18765:18765 -p 13001:13001 \
  -v /path/to/octop-gscore-adapter:/plugin \
  --entrypoint /app/.venv/bin/python \
  ghcr.io/tencentcloud/octop:latest /plugin/tools/live_verify.py

# 3. 现有 OneBot 通道指向 mock NapCat 后重启 Octop
docker restart octop
docker logs -f gscore-connector-mocks
```

`live_verify.py` 会验证：

- 未 @ 机器人的群消息在 Agent 门控前已上报 GsCore；
- GsCore 回复通过原 OneBot 通道发出；
- `echo` 回执、撤回、禁言；
- OneBot 进群元事件。

## 已验证

- 插件以 `kind: hook` 加载，`loaded: true`、`registration_error: null`，版本 `v0.0.1-beta`。
- 通道目录中不再出现 `gscore` 通道类型（仅保留 Octop 原有通道类型）。
- 配置热加载、状态文件、OneBot 通知 hook 安装成功。
- 真实 Octop 进程 + mock GsCore + mock NapCat 全链路通过（见 `live_verify.py`）。
- GsCore v0.11.2 真实 core 的 `MessageReceive`/`MessageSend` 二进制协议已在开发阶段验证；本连接器复用同一客户端与消息模型。

## 已知限制

- Octop 插件体系当前没有 `connector` 类型；本插件使用 `kind: hook` 作为全局生命周期挂载点，不注册 middleware，也不出现在通道/连接器实例列表中。
- 元事件仅 OneBot 11 提供；QQ 官方 / 飞书没有对应的统一 notice 语义。
- QQ 官方与飞书不支持引用段和禁言；QQ 官方撤回未实现，飞书撤回走 REST。
- 富交互段 `markdown` / `buttons` / `template_*` 在 Octop 当前通道上会 warning 后跳过。
- 插件禁用/卸载后建议重启 Octop，确保全局 hook 与连接彻底释放。

## 目录

```
octop-gscore-adapter/
├─ plugin.yaml          # kind: hook, version v0.0.1-beta
├─ main.py              # setup(): 安装全局旁路 hook + 启动连接器
├─ connector.py         # 全局 tap、通道路由、GsCore 双向转换、元事件
├─ client.py            # GsCore WebSocket（二进制帧/重连/队列）
├─ config.py            # 独立 JSON 配置 + 环境变量 + 热加载
├─ models.py            # MessageReceive / MessageSend
├─ content.py           # 内容段互转
├─ tools/               # configure/status/mock/自动化测试
└─ docs/design.md       # 设计说明
```
