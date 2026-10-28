"""Content conversion helpers shared by the three transports."""

from __future__ import annotations

import base64
import binascii
import logging
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from octop_gateway.models import (
    AudioContent,
    ContentPart,
    FileContent,
    ImageContent,
    TextContent,
    VideoContent,
)

from .models import GsMessage

logger = logging.getLogger(__name__)

MAX_LOCAL_FILE_BYTES = 64 * 1024 * 1024
NODE_MARK = "[合并转发]"


def b64_payload(value: str) -> str:
    return value[len("base64://") :] if value.startswith("base64://") else value


def decode_base64(value: str) -> bytes | None:
    try:
        return base64.b64decode(b64_payload(value), validate=False)
    except (ValueError, binascii.Error):
        logger.warning("[GsCore] 非法 base64 媒体数据，已跳过")
        return None


def _local_bytes(uri: str) -> bytes | None:
    path = Path(unquote(urlparse(uri).path or uri[len("file://") :]))
    try:
        if not path.is_file():
            return None
        if path.stat().st_size > MAX_LOCAL_FILE_BYTES:
            logger.warning("[GsCore] 本地文件超过 64MiB，已跳过: %s", path)
            return None
        return path.read_bytes()
    except OSError:
        logger.warning("[GsCore] 无法读取本地文件: %s", path, exc_info=True)
        return None


async def read_part_bytes(channel: Any, part: ContentPart) -> tuple[bytes, str] | None:
    """Read one inbound media part through Octop's media backend."""
    try:
        return await channel.load_media_bytes(part)
    except Exception:  # noqa: BLE001 - backend / platform specific
        logger.warning(
            "[GsCore] 读取入站媒体失败: %s", type(part).__name__, exc_info=True
        )
        return None


async def part_to_gs(
    channel: Any,
    part: ContentPart,
) -> GsMessage | None:
    """Convert one Octop inbound content part to a GsCore segment."""
    if isinstance(part, TextContent):
        return GsMessage("text", part.text) if part.text else None

    if isinstance(part, ImageContent):
        if part.url and part.url.startswith(("http://", "https://")):
            return GsMessage("image", part.url)
        if part.data:
            return GsMessage("image", f"base64://{b64_payload(part.data)}")
        payload = await read_part_bytes(channel, part)
        if payload:
            data, _ = payload
            return GsMessage("image", "base64://" + base64.b64encode(data).decode())
        return None

    if isinstance(part, AudioContent):
        if part.url and part.url.startswith(("http://", "https://")):
            return GsMessage("record", part.url)
        if part.data:
            return GsMessage("record", f"base64://{b64_payload(part.data)}")
        payload = await read_part_bytes(channel, part)
        if payload:
            return GsMessage("record", "base64://" + base64.b64encode(payload[0]).decode())
        return None

    if isinstance(part, VideoContent):
        if part.url and part.url.startswith(("http://", "https://")):
            return GsMessage("video", part.url)
        if part.data:
            return GsMessage("video", f"base64://{b64_payload(part.data)}")
        payload = await read_part_bytes(channel, part)
        if payload:
            return GsMessage("video", "base64://" + base64.b64encode(payload[0]).decode())
        return None

    if isinstance(part, FileContent):
        name = (part.filename or "file").replace("|", "_")
        if part.url and part.url.startswith(("http://", "https://")):
            return GsMessage("file", f"{name}|{part.url}")
        if part.data:
            return GsMessage("file", f"{name}|base64://{b64_payload(part.data)}")
        payload = await read_part_bytes(channel, part)
        if payload:
            return GsMessage("file", f"{name}|base64://{base64.b64encode(payload[0]).decode()}")
        return None

    return None


def _media_source(value: Any) -> tuple[str, str | None] | None:
    """Return ``(url, base64_data)`` for one outbound image/audio/video value."""
    if not isinstance(value, str) or not value:
        return None
    if value.startswith("link://"):
        url = value[len("link://") :]
        return (url, None) if url else None
    if value.startswith("base64://"):
        return ("", b64_payload(value))
    if value.startswith("file://"):
        data = _local_bytes(value)
        return ("", base64.b64encode(data).decode()) if data else None
    if value.startswith(("http://", "https://")):
        return (value, None)
    # Some adapters put a raw base64 string here (legacy behaviour).
    return ("", value)


def _file_source(value: Any) -> tuple[str, str | None, str] | None:
    if not isinstance(value, str) or "|" not in value:
        return None
    name, content = value.split("|", 1)
    name = (name or "file").replace("|", "_")
    if content.startswith("link://"):
        return (content[len("link://") :], None, name)
    if content.startswith(("http://", "https://")):
        return (content, None, name)
    if content.startswith("base64://"):
        return ("", b64_payload(content), name)
    if content.startswith("file://"):
        data = _local_bytes(content)
        return ("", base64.b64encode(data).decode(), name) if data else None
    return ("", content, name)


def flatten_nodes(
    items: list[GsMessage],
    *,
    depth: int = 0,
    max_depth: int = 3,
) -> list[GsMessage]:
    """Flatten ``node`` segments into one sendable list (for non-OneBot)."""
    out: list[GsMessage] = []
    for item in items:
        if item.type == "node":
            out.append(GsMessage("text", NODE_MARK))
            if depth + 1 < max_depth:
                out.extend(
                    flatten_nodes(
                        _node_items(item.data), depth=depth + 1, max_depth=max_depth
                    )
                )
            continue
        out.append(item)
    return out


def _node_items(data: Any) -> list[GsMessage]:
    from .models import as_messages

    return as_messages(data)


def gs_items_to_parts(
    items: list[GsMessage],
    *,
    allow_at: bool = False,
    at_as_text: bool = False,
) -> list[ContentPart]:
    """Convert outbound GsCore segments into Octop content parts.

    Used for the QQ official and Feishu transports.  OneBot has its own native
    segment encoder (it can carry ``at`` / ``reply`` / ``node`` directly).
    Unsupported types are logged and skipped, never raised.
    """
    parts: list[ContentPart] = []
    for item in flatten_nodes(items):
        seg_type = item.type or ""
        data = item.data
        if seg_type in {"text"}:
            if data is not None and str(data):
                parts.append(TextContent(text=str(data)))
        elif seg_type == "image":
            source = _media_source(data)
            if source is None:
                continue
            url, b64 = source
            parts.append(ImageContent(url=url, data=b64))
        elif seg_type == "record":
            source = _media_source(data)
            if source is None:
                continue
            url, b64 = source
            parts.append(AudioContent(url=url, data=b64))
        elif seg_type == "video":
            source = _media_source(data)
            if source is None:
                continue
            url, b64 = source
            parts.append(VideoContent(url=url, data=b64))
        elif seg_type == "file":
            source = _file_source(data)
            if source is None:
                logger.warning("[GsCore] 文件段格式无效（需要 文件名|内容）")
                continue
            url, b64, name = source
            parts.append(FileContent(url=url, data=b64, filename=name))
        elif seg_type == "at":
            target = str(data or "")
            if not target:
                continue
            if allow_at:
                # Feishu supports mention markup inside post text; QQ official
                # does not expose an inline @ segment in the send API used by
                # Octop's built-in channel, so it passes allow_at=False.
                parts.append(TextContent(text=f'<at user_id="{target}"></at>'))
            elif at_as_text:
                parts.append(TextContent(text=f"@{target} "))
        elif seg_type in {"reply", "reply_id"}:
            # QQ official / Feishu built-in channels do not expose a quote
            # primitive.  GsCore still works; just do not send an empty bubble.
            logger.debug("[GsCore] 当前通道不支持引用段，已忽略: %s", seg_type)
        elif seg_type in {"image_size", "group"}:
            continue
        elif seg_type in {
            "markdown",
            "template_markdown",
            "buttons",
            "template_buttons",
        }:
            logger.warning("[GsCore] 当前通道不支持富交互段，已忽略: %s", seg_type)
        elif seg_type.startswith("log"):
            continue
        elif seg_type == "excute_ban_user":
            # Handled by the channel before sending.
            continue
        else:
            logger.warning("[GsCore] 不支持的下发消息段，已忽略: %s", seg_type)
    return parts
