"""GsCore wire models.

The core uses ``msgspec`` structs (``gsuid_core/models.py``).  This plugin keeps
an equivalent but dependency-free representation and decodes defensively: the
core may add fields, and plugins may send content segments this adapter does not
know about.  Unknown fields are ignored, malformed content is skipped with a
warning, and the rest of the packet is still processed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# MessageReceive.user_type is a Literal in the core.
USER_TYPES = {"group", "direct", "channel", "sub_channel"}
# MessageSend.target_type accepts the same vocabulary.
TARGET_TYPES = USER_TYPES


def _as_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return default
    return str(value)


def as_message(value: Any) -> "GsMessage | None":
    """Normalize a dict / GsMessage / malformed item into ``GsMessage``."""
    if isinstance(value, GsMessage):
        return value
    if isinstance(value, dict):
        seg_type = value.get("type")
        return GsMessage(
            type=str(seg_type) if seg_type is not None else None,
            data=value.get("data"),
        )
    return None


def as_messages(value: Any) -> list["GsMessage"]:
    if not isinstance(value, list):
        return []
    out: list[GsMessage] = []
    for item in value:
        normalized = as_message(item)
        if normalized is not None:
            out.append(normalized)
    return out


@dataclass
class GsMessage:
    """One content segment: ``{"type": ..., "data": ...}``."""

    type: str | None = None
    data: Any = None

    def to_dict(self) -> dict[str, Any]:
        return {"type": self.type, "data": self.data}


@dataclass
class MessageReceive:
    """Platform -> core report packet (field names mirror the core struct)."""

    bot_id: str = "Bot"
    bot_self_id: str = ""
    msg_id: str = ""
    user_type: str = "group"
    group_id: str | None = None
    user_id: str = ""
    sender: dict[str, Any] = field(default_factory=dict)
    user_pm: int = 6
    content: list[GsMessage] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        user_type = self.user_type if self.user_type in USER_TYPES else "group"
        return {
            "bot_id": self.bot_id,
            "bot_self_id": self.bot_self_id,
            "msg_id": self.msg_id,
            "user_type": user_type,
            "group_id": self.group_id,
            "user_id": self.user_id,
            "sender": self.sender,
            "user_pm": self.user_pm,
            "content": [item.to_dict() for item in self.content],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MessageReceive":
        return cls(
            bot_id=_as_str(data.get("bot_id"), "Bot"),
            bot_self_id=_as_str(data.get("bot_self_id")),
            msg_id=_as_str(data.get("msg_id")),
            user_type=_as_str(data.get("user_type"), "group"),
            group_id=(
                _as_str(data.get("group_id"))
                if data.get("group_id") is not None
                else None
            ),
            user_id=_as_str(data.get("user_id")),
            sender=(
                dict(data["sender"]) if isinstance(data.get("sender"), dict) else {}
            ),
            user_pm=_safe_int(data.get("user_pm"), 6),
            content=as_messages(data.get("content")),
        )


@dataclass
class MessageSend:
    """Core -> platform outbound packet."""

    bot_id: str = "Bot"
    bot_self_id: str = ""
    msg_id: str = ""
    target_type: str | None = None
    target_id: str | None = None
    content: list[GsMessage] | None = None
    echo: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "bot_id": self.bot_id,
            "bot_self_id": self.bot_self_id,
            "msg_id": self.msg_id,
            "target_type": self.target_type,
            "target_id": self.target_id,
            "content": [item.to_dict() for item in self.content or []],
            "echo": self.echo,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MessageSend":
        raw_content = data.get("content")
        return cls(
            bot_id=_as_str(data.get("bot_id"), "Bot"),
            bot_self_id=_as_str(data.get("bot_self_id")),
            msg_id=_as_str(data.get("msg_id")),
            target_type=(
                _as_str(data.get("target_type"))
                if data.get("target_type") is not None
                else None
            ),
            target_id=(
                _as_str(data.get("target_id"))
                if data.get("target_id") is not None
                else None
            ),
            content=as_messages(raw_content) if raw_content is not None else None,
            echo=(
                _as_str(data.get("echo")) if data.get("echo") is not None else None
            ),
        )


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
