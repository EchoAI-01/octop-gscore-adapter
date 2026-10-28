"""Global GsCore connector.

Unlike a channel plugin, this connector does not own an IM connection. It taps
Octop's *existing* channels before their agent-side group gating, reports every
inbound message to GsCore, and routes GsCore replies back through the same
channel instance. Octop's Agent pipeline is untouched: messages still flow to
the Agent exactly as before, while GsCore independently listens for its own
keyword triggers.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
import weakref
from pathlib import Path
from typing import Any
from urllib.parse import quote

from octop_gateway.channel import BaseChannel
from octop_gateway.models import (
    ChannelSubject,
    ContentPart,
    InboundMessage,
    TextContent,
)

from .client import GsCoreClient
from .config import ConnectorConfig, split_values
from .content import NODE_MARK, gs_items_to_parts, part_to_gs
from .models import GsMessage, MessageReceive, MessageSend, as_messages

logger = logging.getLogger(__name__)

_FEISHU_API_BASE = "https://open.feishu.cn/open-apis"
_CONTROL_CHANNEL_TYPES = frozenset({"dashboard", "cli", "web", "bridge"})
_NODE_MAX_DEPTH = 3


def _strip_subject(subject_id: str) -> str:
    for prefix in ("group:", "user:", "channel:", "room:"):
        if subject_id.startswith(prefix):
            return subject_id[len(prefix) :]
    return subject_id


def _short(exc: BaseException, limit: int = 240) -> str:
    text = str(exc).strip() or exc.__class__.__name__
    return text if len(text) <= limit else text[:limit] + "..."


def _id_value(value: Any) -> Any:
    text = str(value or "").strip()
    if text.isascii() and text.isdigit():
        return int(text)
    return text


def _as_gs(value: Any) -> GsMessage | None:
    from .models import as_message

    return as_message(value)


def _node_data(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _flatten_node_data(items: list[Any], depth: int = 0) -> list[GsMessage]:
    out: list[GsMessage] = []
    for item in items:
        message = item if isinstance(item, GsMessage) else _as_gs(item)
        if message is None:
            continue
        if message.type == "node":
            out.append(GsMessage("text", NODE_MARK))
            if depth + 1 < _NODE_MAX_DEPTH:
                out.extend(_flatten_node_data(_node_data(message.data), depth + 1))
            continue
        out.append(message)
    return out


def _node_preview(items: list[GsMessage]) -> str:
    lines = [NODE_MARK]
    for item in items:
        if item.type == "text" and item.data is not None:
            text = str(item.data).strip()
            if text:
                lines.append(text)
        elif item.type == "image":
            lines.append("[图片]")
        elif item.type == "record":
            lines.append("[语音]")
        elif item.type == "video":
            lines.append("[视频]")
        elif item.type == "file":
            lines.append("[文件]")
    return "\n".join(lines)


def install_global_hooks() -> None:
    """Install the inbound tap and graceful-shutdown hook exactly once.

    The active connector lives on ``BaseChannel._gscore_connector`` rather than
    in this module, so a plugin reload can swap it without stacking wrappers.
    """
    if not getattr(BaseChannel, "_gscore_handle_hook", False):
        original_handle = BaseChannel.handle_inbound

        async def handle_inbound_with_gscore(self: BaseChannel, raw_payload: Any) -> None:
            connector = getattr(BaseChannel, "_gscore_connector", None)
            if connector is not None:
                connector.schedule_observe(self, raw_payload)
            return await original_handle(self, raw_payload)

        BaseChannel.handle_inbound = handle_inbound_with_gscore  # type: ignore[method-assign]
        BaseChannel._gscore_handle_hook = True  # type: ignore[attr-defined]

    try:
        from octop.infra.server import OctopServer
    except Exception:  # pragma: no cover - only in non-Octop hosts
        OctopServer = None

    if OctopServer is not None and not getattr(OctopServer, "_gscore_stop_hook", False):
        original_stop = OctopServer.stop

        async def stop_with_gscore(self: Any) -> None:
            connector = getattr(BaseChannel, "_gscore_connector", None)
            if connector is not None:
                with contextlib.suppress(Exception):
                    await connector.stop()
            return await original_stop(self)

        OctopServer.stop = stop_with_gscore  # type: ignore[method-assign]
        OctopServer._gscore_stop_hook = True  # type: ignore[attr-defined]

    try:
        from octop_gateway.manager import ChannelManager
    except Exception:  # pragma: no cover
        return

    if not getattr(ChannelManager, "_gscore_register_hook", False):
        original_register = ChannelManager._register_channel

        async def register_with_gscore(self: Any, channel: BaseChannel) -> str:
            channel_id = await original_register(self, channel)
            connector = getattr(BaseChannel, "_gscore_connector", None)
            if connector is not None and connector.is_allowed_channel(channel):
                connector.register_channel(channel)
            return channel_id

        ChannelManager._register_channel = register_with_gscore  # type: ignore[method-assign]
        ChannelManager._gscore_register_hook = True  # type: ignore[attr-defined]


class GsCoreConnector:
    """Observe every allowed Octop channel and bridge it to GsCore."""

    def __init__(self, config_path: Path, *, version: str) -> None:
        self.config_path = config_path
        self.version = version
        self._config = ConnectorConfig.load(config_path)
        self._client = GsCoreClient(self._config, self._handle_send)
        self._channels: weakref.WeakValueDictionary[str, BaseChannel] = (
            weakref.WeakValueDictionary()
        )
        self._targets: dict[tuple[str, str, str], dict[str, Any]] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._watch_task: asyncio.Task[None] | None = None
        self._running = False
        self._last_config_mtime_ns = 0
        self._last_message_at = 0.0
        self._last_error: str | None = None
        self._last_status = ""
        self._status_path = config_path.parent / "status.json"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    @property
    def config(self) -> ConnectorConfig:
        return self._config

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._last_config_mtime_ns = self._config_mtime_ns()
        self._write_status()
        if self._config.enabled:
            missing = self._config.missing_fields()
            if missing:
                self._last_error = f"配置不完整: {', '.join(missing)}"
                logger.error("[GsCoreConnector] %s", self._last_error)
            else:
                await self._client.start()
        else:
            logger.info(
                "[GsCoreConnector] 未启用；请编辑 %s 后自动热加载",
                self.config_path,
            )
        self._watch_task = asyncio.create_task(
            self._watch_loop(), name="gscore-connector-watch"
        )
        logger.info(
            "[GsCoreConnector] started: enabled=%s target=%s config=%s",
            self._config.enabled,
            self._config.status_label,
            self.config_path,
        )

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        watch, self._watch_task = self._watch_task, None
        if watch is not None:
            watch.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await watch
        for task in list(self._tasks):
            task.cancel()
        self._tasks.clear()
        await self._client.stop()
        self._write_status(force=True)
        logger.info("[GsCoreConnector] stopped")

    def schedule_observe(self, channel: BaseChannel, raw_payload: Any) -> None:
        if not self._running:
            return
        task = asyncio.create_task(self.observe_raw(channel, raw_payload))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def register_channel(self, channel: BaseChannel) -> None:
        """Remember a live channel so proactive GsCore messages can route."""
        self._channels[channel.channel_id] = channel
        self._write_status()

    def is_allowed_channel(self, channel: BaseChannel) -> bool:
        return self._allowed_channel(channel)

    # ------------------------------------------------------------------
    # Channel filtering / normalization
    # ------------------------------------------------------------------
    def _allowed_channel(self, channel: BaseChannel) -> bool:
        channel_type = str(getattr(channel, "channel_type", "") or "")
        if channel_type in _CONTROL_CHANNEL_TYPES:
            return False
        allowed = self._config.allowed_channel_types
        if not allowed:
            return True
        return channel_type in allowed

    async def observe_raw(self, channel: BaseChannel, raw_payload: Any) -> None:
        try:
            if not self._allowed_channel(channel):
                return
            message = (
                raw_payload
                if isinstance(raw_payload, InboundMessage)
                else channel.parse_inbound(raw_payload)
            )
            if not isinstance(message, InboundMessage):
                return
            await self.observe_message(channel, message)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - never break the channel pipeline
            self._last_error = f"解析入站消息失败: {_short(exc)}"
            logger.debug("[GsCoreConnector] 解析入站消息失败", exc_info=True)

    async def observe_message(
        self,
        channel: BaseChannel,
        message: InboundMessage,
    ) -> None:
        metadata = message.metadata or {}
        if metadata.get("monitor") or metadata.get("poke"):
            # OneBot synthetic monitor/poke bubbles are not user messages; the
            # poke meta event is reported separately.
            return
        self._channels[channel.channel_id] = channel
        self._remember_target(channel, message)
        self._last_message_at = time.time()
        if not self._config.enabled:
            self._write_status()
            return
        receive = await self._build_receive(channel, message)
        if receive is None or not receive.content:
            self._write_status()
            return
        if not self._client.is_running:
            with contextlib.suppress(Exception):
                await self._client.start()
        await self._client.report(receive)
        self._write_status()

    def _remember_target(self, channel: BaseChannel, message: InboundMessage) -> None:
        is_group, target_id = self._message_target(channel, message)
        if not target_id:
            return
        key = (channel.channel_id, "group" if is_group else "direct", target_id)
        self._targets[key] = dict(message.metadata or {})

    def _message_target(
        self,
        channel: BaseChannel,
        message: InboundMessage,
    ) -> tuple[bool, str]:
        metadata = message.metadata or {}
        subject = message.channel_subject
        subject_id = str(subject.subject_id if subject is not None else "")
        is_group = self._is_group_message(channel, message)
        if is_group:
            return True, self._group_id(channel, metadata, subject_id)
        return False, self._user_id(metadata, subject_id)

    def _is_group_message(self, channel: BaseChannel, message: InboundMessage) -> bool:
        metadata = message.metadata or {}
        subject = message.channel_subject
        channel_type = str(getattr(channel, "channel_type", ""))
        if channel_type == "qq":
            return str(metadata.get("msg_type") or "") in {"group", "channel"}
        if subject is not None and subject.chat_type == "group":
            return True
        if metadata.get("chat_type") == "group":
            return True
        if metadata.get("is_group") is True:
            return True
        return str(metadata.get("msg_type") or "") in {"group", "channel"}

    @staticmethod
    def _group_id(
        channel: BaseChannel,
        metadata: dict[str, Any],
        subject_id: str,
    ) -> str:
        channel_type = str(getattr(channel, "channel_type", ""))
        if channel_type == "feishu":
            return str(metadata.get("chat_id") or "") or _strip_subject(subject_id)
        if channel_type == "qq":
            return str(
                metadata.get("group_openid")
                or metadata.get("conversation_id")
                or metadata.get("channel_id_native")
                or metadata.get("group_id")
                or ""
            ) or _strip_subject(subject_id)
        return str(metadata.get("group_id") or "") or _strip_subject(subject_id)

    @staticmethod
    def _user_id(metadata: dict[str, Any], subject_id: str) -> str:
        return str(
            metadata.get("sender_id")
            or metadata.get("user_id")
            or metadata.get("member_openid")
            or metadata.get("open_id")
            or ""
        ) or _strip_subject(subject_id)

    def _platform_bot_id(
        self,
        channel: BaseChannel,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        metadata = metadata or {}
        channel_type = str(getattr(channel, "channel_type", "") or "")
        if channel_type == "onebot11":
            return self._config.onebot_bot_id or "onebot"
        if channel_type == "qq":
            msg_type = str(metadata.get("msg_type") or "")
            if msg_type in {"channel", "direct"}:
                return self._config.qq_guild_bot_id or "qqguild"
            return self._config.qq_group_bot_id or "qqgroup"
        if channel_type == "feishu":
            return self._config.feishu_bot_id or "feishu"
        return channel_type or "unknown"

    def _channel_matches_bot_id(self, channel: BaseChannel, bot_id: str) -> bool:
        base = str(bot_id or "").split(":", 1)[0]
        channel_type = str(getattr(channel, "channel_type", "") or "")
        if channel_type == "onebot11":
            return base == (self._config.onebot_bot_id or "onebot").split(":", 1)[0]
        if channel_type == "qq":
            return base in {
                (self._config.qq_group_bot_id or "qqgroup").split(":", 1)[0],
                (self._config.qq_guild_bot_id or "qqguild").split(":", 1)[0],
            }
        if channel_type == "feishu":
            return base in {
                (self._config.feishu_bot_id or "feishu").split(":", 1)[0],
                "lark",
            }
        return base == channel_type

    # ------------------------------------------------------------------
    # Inbound -> MessageReceive
    # ------------------------------------------------------------------
    async def _build_receive(
        self,
        channel: BaseChannel,
        inbound: InboundMessage,
    ) -> MessageReceive | None:
        metadata = inbound.metadata or {}
        is_group = self._is_group_message(channel, inbound)
        subject = inbound.channel_subject
        subject_id = str(subject.subject_id if subject is not None else "")
        bot_self_id = channel.channel_id
        group_id: str | None = None
        if is_group:
            group_id = self._group_id(channel, metadata, subject_id) or None
        elif metadata.get("temp_group_id"):
            group_id = str(metadata.get("temp_group_id")) or None
        user_id = self._user_id(metadata, subject_id)
        msg_id = str(metadata.get("message_id") or metadata.get("msg_id") or "")
        if not msg_id:
            msg_id = str(inbound.channel_session_id or "")

        role = str(metadata.get("role") or "").lower()
        if user_id and user_id in self._config.admin_ids:
            user_pm = 1
        elif role == "owner":
            user_pm = 2
        elif role == "admin":
            user_pm = 3
        else:
            user_pm = 6

        content = await self._build_content(
            channel,
            inbound,
            is_group=is_group,
            bot_self_id=bot_self_id,
        )
        if not content:
            return None
        return MessageReceive(
            bot_id=self._platform_bot_id(channel, metadata),
            bot_self_id=bot_self_id,
            msg_id=msg_id,
            user_type="group" if is_group else "direct",
            group_id=group_id,
            user_id=user_id,
            sender={
                "nickname": str(metadata.get("sender_name") or ""),
                "avatar": self._avatar(channel, user_id, metadata),
            },
            user_pm=user_pm,
            content=content,
        )

    def _avatar(
        self,
        channel: BaseChannel,
        user_id: str,
        metadata: dict[str, Any],
    ) -> str:
        supplied = str(metadata.get("avatar") or "")
        if supplied:
            return supplied
        if not user_id:
            return ""
        channel_type = str(getattr(channel, "channel_type", ""))
        if channel_type == "onebot11":
            return f"https://q1.qlogo.cn/g?b=qq&nk={user_id}&s=640"
        if channel_type == "qq":
            app_id = str(getattr(getattr(channel, "_config", None), "app_id", "") or "")
            if app_id:
                return f"https://q.qlogo.cn/qqapp/{app_id}/{user_id}/100"
        return ""

    async def _build_content(
        self,
        channel: BaseChannel,
        inbound: InboundMessage,
        *,
        is_group: bool,
        bot_self_id: str,
    ) -> list[GsMessage]:
        metadata = inbound.metadata or {}
        items: list[GsMessage] = []

        if is_group and bool(metadata.get("bot_mentioned")) and bot_self_id:
            items.append(GsMessage("at", bot_self_id))

        for part in inbound.content:
            item = await part_to_gs(channel, part)
            if item is not None:
                items.append(item)

        seen_at: set[str] = {bot_self_id} if bot_self_id else set()
        for mention in metadata.get("mentions") or []:
            if not isinstance(mention, dict):
                continue
            target = str(
                mention.get("qq")
                or mention.get("open_id")
                or mention.get("id")
                or ""
            )
            if not target or target in seen_at or mention.get("is_you") is True:
                continue
            seen_at.add(target)
            items.append(GsMessage("at", target))

        node_items = as_messages(metadata.get("gscore_node"))
        if node_items:
            items.append(GsMessage("node", [item.to_dict() for item in node_items]))

        reply_id = str(metadata.get("reply_to") or metadata.get("reply_id") or "")
        reply_text = str(metadata.get("reply_text") or "")
        reply_images = as_messages(metadata.get("reply_images"))
        reply_node = as_messages(metadata.get("reply_node"))
        if reply_node:
            preview = _node_preview(reply_node)
            if not reply_text:
                reply_text = preview
            elif NODE_MARK not in reply_text:
                reply_text = f"{NODE_MARK}\n{reply_text}"
        if reply_id:
            items.append(GsMessage("reply_id", reply_id))
            if reply_text:
                items.append(GsMessage("reply", reply_text))
        for image in reply_images:
            items.append(image)
        if reply_node:
            items.append(GsMessage("node", [item.to_dict() for item in reply_node]))
        return items

    async def observe_onebot_notice(
        self,
        channel: BaseChannel,
        payload: dict[str, Any],
    ) -> None:
        if not self._config.enabled:
            return
        try:
            notice_type = str(payload.get("notice_type") or "")
            sub_type = str(payload.get("sub_type") or "")
            if notice_type == "group_increase":
                await self._report_meta(
                    channel,
                    "user_join_group",
                    {
                        "user_id": str(payload.get("user_id") or ""),
                        "group_id": str(payload.get("group_id") or ""),
                        "operator_id": str(payload.get("operator_id") or ""),
                        "sub_type": sub_type,
                    },
                )
            elif notice_type == "group_decrease":
                await self._report_meta(
                    channel,
                    "user_exit_group",
                    {
                        "user_id": str(payload.get("user_id") or ""),
                        "group_id": str(payload.get("group_id") or ""),
                        "operator_id": str(payload.get("operator_id") or ""),
                        "sub_type": sub_type,
                    },
                )
            elif notice_type == "notify" and sub_type == "poke":
                data: dict[str, Any] = {
                    "user_id": str(payload.get("user_id") or ""),
                    "target_id": str(payload.get("target_id") or ""),
                }
                if payload.get("group_id") is not None:
                    data["group_id"] = str(payload.get("group_id"))
                await self._report_meta(channel, "poke", data)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            logger.debug("[GsCoreConnector] meta 上报失败", exc_info=True)

    async def _report_meta(
        self,
        channel: BaseChannel,
        event_name: str,
        data: dict[str, Any],
    ) -> None:
        group_id = str(data.get("group_id") or "") or None
        user_id = str(data.get("user_id") or "")
        receive = MessageReceive(
            bot_id=self._platform_bot_id(channel),
            bot_self_id=channel.channel_id,
            user_type="group" if group_id else "direct",
            group_id=group_id,
            user_id=user_id,
            sender={},
            user_pm=1 if user_id and user_id in self._config.admin_ids else 6,
            msg_id="",
            content=[GsMessage(f"meta-{event_name}", data)],
        )
        if not self._client.is_running:
            await self._client.start()
        await self._client.report(receive)

    # ------------------------------------------------------------------
    # Outbound MessageSend -> existing channel
    # ------------------------------------------------------------------
    async def _handle_send(self, msg: MessageSend) -> None:
        content = list(msg.content or [])
        channel = self._select_channel(msg)
        if not content:
            if msg.echo:
                await self._send_receipt(msg, None)
            return

        if len(content) == 1 and content[0].type == "excute_delete_message":
            if channel is not None:
                data = content[0].data if isinstance(content[0].data, dict) else {}
                message_id = data.get("message_id")
                if message_id is not None:
                    await self._delete(
                        channel,
                        target_type=msg.target_type,
                        target_id=str(msg.target_id or ""),
                        message_id=str(message_id),
                    )
            return

        remaining: list[GsMessage] = []
        for item in content:
            if item.type == "excute_ban_user":
                if channel is not None and isinstance(item.data, dict):
                    await self._ban(channel, item.data)
                continue
            remaining.append(item)
        if not remaining:
            if msg.echo:
                await self._send_receipt(msg, None)
            return

        if channel is None:
            self._last_error = (
                f"找不到 bot_self_id={msg.bot_self_id!r} bot_id={msg.bot_id!r} 对应的 Octop 通道"
            )
            logger.warning("[GsCoreConnector] %s", self._last_error)
            if msg.echo:
                await self._send_receipt(msg, None)
            return
        target_id = str(msg.target_id or "")
        if not target_id:
            logger.warning("[GsCoreConnector] MessageSend 缺少 target_id")
            if msg.echo:
                await self._send_receipt(msg, None)
            return

        recall_id: str | list[str] | None = None
        try:
            recall_id = await self._send_to_channel(channel, msg, remaining)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - control the echo path
            self._last_error = f"发送失败: {_short(exc)}"
            logger.exception(
                "[GsCoreConnector] 发送失败: channel=%s bot_id=%s target=%s/%s",
                channel.channel_id,
                msg.bot_id,
                msg.target_type,
                target_id,
            )
        finally:
            if msg.echo:
                await self._send_receipt(msg, recall_id)

    def _select_channel(self, msg: MessageSend) -> BaseChannel | None:
        channel = self._channels.get(str(msg.bot_self_id or ""))
        if channel is not None:
            return channel
        candidates = [
            item
            for item in list(self._channels.values())
            if self._channel_matches_bot_id(item, msg.bot_id)
        ]
        if not candidates and len(self._channels) == 1:
            candidates = list(self._channels.values())
        if not candidates:
            return None
        connected = [
            item for item in candidates if bool(getattr(item, "is_connected", False))
        ]
        return (connected or candidates)[0]

    async def _send_to_channel(
        self,
        channel: BaseChannel,
        msg: MessageSend,
        content: list[GsMessage],
    ) -> str | list[str] | None:
        is_group = msg.target_type in {"group", "channel", "sub_channel"}
        target_id = str(msg.target_id or "")
        subject = self._build_subject(channel, msg, target_id, is_group=is_group)

        if str(getattr(channel, "channel_type", "")) == "onebot11" and hasattr(
            channel, "push_segments"
        ):
            segments, nodes = self._onebot_content(channel, content)
            if nodes:
                if is_group:
                    await channel.push_forward(subject, nodes)  # type: ignore[attr-defined]
                else:
                    # OneBot's private forward API is not exposed by the
                    # onebot11 plugin's helper; flatten private nodes instead.
                    for node in nodes:
                        data = node.get("data") if isinstance(node.get("data"), dict) else {}
                        for seg in data.get("content") or []:
                            if isinstance(seg, dict):
                                segments.append(seg)
            if segments:
                await channel.push_segments(subject, segments)  # type: ignore[attr-defined]
            return None

        parts: list[ContentPart] = gs_items_to_parts(
            content,
            allow_at=str(getattr(channel, "channel_type", "")) == "feishu",
            at_as_text=False,
        )
        if not parts:
            return None
        await channel._acquire_rate_slot()
        await channel._send_content(subject, parts)
        return None

    def _build_subject(
        self,
        channel: BaseChannel,
        msg: MessageSend,
        target_id: str,
        *,
        is_group: bool,
    ) -> ChannelSubject:
        key = (channel.channel_id, "group" if is_group else "direct", target_id)
        cached = dict(self._targets.get(key, {}))
        channel_type = str(getattr(channel, "channel_type", ""))
        meta: dict[str, Any] = cached
        if is_group:
            meta["chat_type"] = "group"
            meta["is_group"] = True
            meta.setdefault("message_type", "group")
            if channel_type == "onebot11":
                meta.setdefault("group_id", target_id)
                subject_id = f"group:{target_id}"
            else:
                meta.setdefault("chat_id", target_id)
                meta.setdefault("to_handle", target_id)
                subject_id = target_id
            if channel_type == "qq":
                msg_type = str(meta.get("msg_type") or "")
                guild_id = (self._config.qq_guild_bot_id or "qqguild").split(":", 1)[0]
                is_guild = msg_type == "channel" or str(msg.bot_id).split(":", 1)[0] == guild_id
                if is_guild:
                    meta["msg_type"] = "channel"
                    meta.setdefault("channel_id_native", target_id)
                else:
                    meta["msg_type"] = "group"
                    meta.setdefault("group_openid", target_id)
        else:
            meta["chat_type"] = "dm"
            meta["is_group"] = False
            meta.setdefault("user_id", target_id)
            meta.setdefault("sender_id", target_id)
            subject_id = target_id
            if channel_type == "onebot11":
                meta["chat_type"] = "dm"
            elif channel_type == "qq":
                guild_id = (self._config.qq_guild_bot_id or "qqguild").split(":", 1)[0]
                if (
                    str(meta.get("msg_type") or "") == "direct"
                    or str(msg.bot_id).split(":", 1)[0] == guild_id
                ):
                    meta["msg_type"] = "direct"
                    meta.setdefault("guild_id", cached.get("guild_id") or "")
                else:
                    meta["msg_type"] = "c2c"
                    meta.setdefault("user_openid", target_id)
            elif channel_type == "feishu":
                meta.setdefault("to_handle", target_id)
                meta.setdefault("chat_id", cached.get("chat_id") or target_id)

        anchor = str(msg.msg_id or cached.get("message_id") or cached.get("msg_id") or "")
        if anchor:
            meta["msg_id"] = anchor
            meta["message_id"] = anchor
        return ChannelSubject(
            subject_id=subject_id,
            chat_type="group" if is_group else "direct",
            metadata=meta,
        )

    # ------------------------------------------------------------------
    # OneBot native segments (at / reply / node / media)
    # ------------------------------------------------------------------
    def _onebot_content(
        self,
        channel: BaseChannel,
        content: list[GsMessage],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        segments: list[dict[str, Any]] = []
        nodes: list[dict[str, Any]] = []
        for item in content:
            if item.type == "node":
                built = self._onebot_nodes(channel, _node_data(item.data))
                nodes.extend(built)
                continue
            segment = self._onebot_segment(item)
            if segment is not None:
                segments.append(segment)
        return segments, nodes

    def _onebot_nodes(
        self,
        channel: BaseChannel,
        items: list[Any],
    ) -> list[dict[str, Any]]:
        nodes: list[dict[str, Any]] = []
        self_id = str(getattr(channel, "_self_id", "") or "0")
        for item in _flatten_node_data(items):
            segment = self._onebot_segment(item)
            if segment is None:
                continue
            nodes.append(
                {
                    "type": "node",
                    "data": {
                        "name": "GsCore",
                        "uin": _id_value(self_id),
                        "content": [segment],
                    },
                }
            )
        return nodes

    def _onebot_segment(self, item: GsMessage) -> dict[str, Any] | None:
        seg_type = item.type or ""
        data = item.data
        if seg_type == "text":
            text = str(data or "")
            return {"type": "text", "data": {"text": text}} if text else None
        if seg_type == "at":
            target = str(data or "")
            return {"type": "at", "data": {"qq": target}} if target else None
        if seg_type in {"reply", "reply_id"}:
            target = str(data or "")
            return {"type": "reply", "data": {"id": target}} if target else None
        if seg_type in {"image", "record", "video"}:
            return _onebot_media_segment(seg_type, data)
        if seg_type == "file":
            if not isinstance(data, str) or "|" not in data:
                logger.warning("[GsCoreConnector] 文件段格式无效（需要 文件名|内容）")
                return None
            name, value = data.split("|", 1)
            if value.startswith("link://"):
                value = value[len("link://") :]
            return {"type": "file", "data": {"file": value, "name": name}}
        if seg_type in {"image_size", "group", "excute_ban_user"}:
            return None
        if seg_type in {"markdown", "template_markdown", "buttons", "template_buttons"}:
            logger.warning("[GsCoreConnector] 当前 OneBot 通道不支持富交互段: %s", seg_type)
            return None
        if seg_type.startswith("log"):
            return None
        logger.warning("[GsCoreConnector] 不支持的下发消息段: %s", seg_type)
        return None

    # ------------------------------------------------------------------
    # Controls
    # ------------------------------------------------------------------
    async def _delete(
        self,
        channel: BaseChannel,
        *,
        target_type: str | None,
        target_id: str,
        message_id: str,
    ) -> None:
        channel_type = str(getattr(channel, "channel_type", ""))
        try:
            if channel_type == "onebot11" and hasattr(channel, "_call_api"):
                await channel._call_api(  # type: ignore[attr-defined]
                    "delete_msg", {"message_id": _id_value(message_id)}
                )
                return
            if channel_type == "feishu":
                http = await channel._ensure_http()
                headers = await channel._get_auth_headers()
                url = f"{_FEISHU_API_BASE}/im/v1/messages/{quote(message_id, safe='')}"
                async with http.delete(url, headers=headers) as resp:
                    if resp.status >= 300:
                        body = await resp.text()
                        logger.warning(
                            "[GsCoreConnector] 飞书撤回失败: status=%s body=%s",
                            resp.status,
                            body[:200],
                        )
                return
            logger.warning(
                "[GsCoreConnector] 通道 %s 暂不支持撤回消息", channel_type
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("[GsCoreConnector] 撤回消息失败: %s", exc)

    async def _ban(self, channel: BaseChannel, data: dict[str, Any]) -> None:
        user_id = data.get("user_id")
        group_id = data.get("group_id")
        duration = data.get("duration")
        if user_id is None or group_id is None:
            return
        if not (
            isinstance(duration, int)
            or (isinstance(duration, str) and duration.isdigit())
        ):
            return
        if str(getattr(channel, "channel_type", "")) != "onebot11" or not hasattr(
            channel, "_call_api"
        ):
            logger.warning("[GsCoreConnector] 当前通道暂不支持禁言")
            return
        try:
            await channel._call_api(  # type: ignore[attr-defined]
                "set_group_ban",
                {
                    "group_id": _id_value(group_id),
                    "user_id": _id_value(user_id),
                    "duration": int(duration),
                },
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("[GsCoreConnector] 禁言失败: %s", exc)

    async def _send_receipt(
        self,
        msg: MessageSend,
        recall_id: str | list[str] | None,
    ) -> None:
        receipt = MessageReceive(
            bot_id=msg.bot_id,
            bot_self_id=msg.bot_self_id,
            user_type="direct",
            group_id=None,
            user_id="",
            sender={},
            user_pm=6,
            msg_id="",
            content=[
                GsMessage(
                    "recall_message_id",
                    {"echo": msg.echo, "id": recall_id},
                )
            ],
        )
        await self._client.report(receipt)

    # ------------------------------------------------------------------
    # Config reload / status
    # ------------------------------------------------------------------
    def _config_mtime_ns(self) -> int:
        try:
            return self.config_path.stat().st_mtime_ns
        except OSError:
            return 0

    async def _watch_loop(self) -> None:
        while self._running:
            await asyncio.sleep(max(1.0, self._config.status_interval_seconds))
            self._ensure_notice_hook()
            try:
                mtime = self._config_mtime_ns()
                if mtime and mtime != self._last_config_mtime_ns:
                    self._last_config_mtime_ns = mtime
                    await self._reload_config()
            except Exception as exc:  # noqa: BLE001
                self._last_error = f"配置热加载失败: {_short(exc)}"
                logger.warning("[GsCoreConnector] %s", self._last_error)
            self._write_status()

    async def _reload_config(self) -> None:
        new_config = ConnectorConfig.load(self.config_path)
        old_config = self._config
        self._config = new_config
        await self._client.stop()
        self._client = GsCoreClient(new_config, self._handle_send)
        self._last_error = None
        if new_config.enabled:
            missing = new_config.missing_fields()
            if missing:
                self._last_error = f"配置不完整: {', '.join(missing)}"
                logger.error("[GsCoreConnector] %s", self._last_error)
            else:
                await self._client.start()
        logger.info(
            "[GsCoreConnector] 配置已热加载: enabled %s -> %s, target=%s",
            old_config.enabled,
            new_config.enabled,
            new_config.status_label,
        )
        self._write_status(force=True)

    def _ensure_notice_hook(self) -> None:
        try:
            from octop_gateway.channels import BUILTIN_CHANNELS
        except Exception:
            return
        channel_cls = BUILTIN_CHANNELS.get("onebot11")
        if channel_cls is None or getattr(channel_cls, "_gscore_notice_tapped", False):
            return
        original = getattr(channel_cls, "_handle_notice_event", None)
        if original is None:
            return

        async def tapped(self: Any, payload: dict[str, Any], *args: Any, **kwargs: Any) -> Any:
            connector = getattr(BaseChannel, "_gscore_connector", None)
            if connector is not None:
                await connector.observe_onebot_notice(self, payload)
            return await original(self, payload, *args, **kwargs)

        channel_cls._handle_notice_event = tapped  # type: ignore[attr-defined]
        channel_cls._gscore_notice_tapped = True  # type: ignore[attr-defined]
        logger.info("[GsCoreConnector] OneBot 11 notice hook installed")

    def _status_payload(self) -> dict[str, Any]:
        channels = [
            {
                "id": item.channel_id,
                "type": str(getattr(item, "channel_type", "")),
                "connected": bool(getattr(item, "is_connected", False)),
            }
            for item in list(self._channels.values())
        ]
        return {
            "version": self.version,
            "enabled": self._config.enabled,
            "connected": self._client.is_connected,
            "target": self._config.status_label,
            "route_bot_id": self._config.gscore_bot_id,
            "config_path": str(self.config_path),
            "config_error": self._config.missing_fields(),
            "runtime_error": (
                self._client.runtime_error or self._last_error
                if self._config.enabled
                else self._last_error
            ),
            "channels": channels,
            "last_message_at": self._last_message_at,
            "updated_at": time.time(),
        }

    def _write_status(self, *, force: bool = False) -> None:
        try:
            payload = self._status_payload()
            encoded = json.dumps(payload, ensure_ascii=False, indent=2)
            if not force and encoded == self._last_status:
                return
            self._status_path.parent.mkdir(parents=True, exist_ok=True)
            temp = self._status_path.with_suffix(".json.tmp")
            temp.write_text(encoded + "\n", encoding="utf-8")
            temp.replace(self._status_path)
            self._last_status = encoded
        except OSError:
            logger.debug("[GsCoreConnector] 写状态文件失败", exc_info=True)


def _onebot_media_segment(seg_type: str, value: Any) -> dict[str, Any] | None:
    if not isinstance(value, str) or not value:
        return None
    if value.startswith("link://"):
        file_value = value[len("link://") :]
    elif value.startswith("file://"):
        file_value = value
    elif value.startswith("base64://"):
        file_value = value
    elif value.startswith(("http://", "https://")):
        file_value = value
    else:
        file_value = "base64://" + value
    return {"type": seg_type, "data": {"file": file_value}}


__all__ = ["GsCoreConnector", "install_global_hooks"]
