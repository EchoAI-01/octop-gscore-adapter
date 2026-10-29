"""Connector configuration.

A connector has no Octop channel row, so its settings live in a JSON file
outside the plugin directory (survives plugin reinstall) and may be overridden
by environment variables.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Self

_TRUE = {"1", "true", "yes", "y", "on", "enable", "enabled"}
_FALSE = {"0", "false", "no", "n", "off", "disable", "disabled"}


def to_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in _TRUE:
        return True
    if text in _FALSE:
        return False
    return default


def to_int(value: Any, default: int) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def to_float(value: Any, default: float) -> float:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return default


def split_values(raw: Any) -> tuple[str, ...]:
    if isinstance(raw, (list, tuple, set)):
        values = [str(item).strip() for item in raw]
    else:
        values = re.split(r"[,;\n\r]+", str(raw or ""))
    return tuple(item.strip() for item in values if item.strip())


def default_config_path() -> Path:
    """Return the persistent config file path.

    ``OCTOP_GSCORE_CONFIG`` wins; otherwise ``$OCTOP_HOME/gscore/config.json``
    (``/data/.octop/gscore/config.json`` in the official Docker image).
    """
    override = os.environ.get("OCTOP_GSCORE_CONFIG", "").strip()
    if override:
        return Path(override).expanduser()
    root = os.environ.get("OCTOP_HOME", "").strip()
    base = Path(root).expanduser() if root else Path.home() / ".octop"
    return base / "gscore" / "config.json"


def default_status_path(config_path: Path) -> Path:
    return config_path.parent / "status.json"


@dataclass
class ConnectorConfig:
    """Global GsCore connector settings (not tied to any Octop channel)."""

    enabled: bool = False

    # GsCore connection
    gscore_host: str = "localhost"
    gscore_port: int = 8765
    gscore_ws_url: str = ""
    gscore_ws_token: str = ""
    gscore_bot_id: str = "AstrBot"
    gscore_max_retry: int = 30
    gscore_reconnect_seconds: float = 5.0
    gscore_connect_timeout: float = 8.0

    # Platform ids reported to GsCore (reference/AstrBot naming)
    onebot_bot_id: str = "onebot"
    qq_group_bot_id: str = "qq_official"
    feishu_bot_id: str = "lark"

    # Reference GSCORE_ONLY_PREFIXES: report to GsCore and stop the Agent path
    gscore_only_prefixes: str = ""

    # Permissions
    admin_user_ids: str = ""

    # Which Octop channel types to observe; empty means every non-control channel.
    channel_types: str = ""

    # Observability
    status_interval_seconds: float = 2.0

    def __post_init__(self) -> None:
        for name in (
            "gscore_host",
            "gscore_ws_url",
            "gscore_ws_token",
            "gscore_bot_id",
            "onebot_bot_id",
            "qq_group_bot_id",
            "feishu_bot_id",
            "gscore_only_prefixes",
            "admin_user_ids",
            "channel_types",
        ):
            raw = getattr(self, name)
            if isinstance(raw, (list, tuple, set)):
                raw = ",".join(str(item) for item in raw)
            setattr(self, name, str(raw or "").strip())

        self.enabled = to_bool(self.enabled, False)
        self.gscore_port = to_int(self.gscore_port, 8765)
        self.gscore_max_retry = to_int(self.gscore_max_retry, 30)
        self.gscore_reconnect_seconds = to_float(self.gscore_reconnect_seconds, 5.0)
        self.gscore_connect_timeout = to_float(self.gscore_connect_timeout, 8.0)
        self.status_interval_seconds = to_float(self.status_interval_seconds, 2.0)

        if not 1 <= self.gscore_port <= 65535:
            self.gscore_port = 8765
        if self.gscore_max_retry < -1:
            self.gscore_max_retry = 30
        if self.gscore_reconnect_seconds < 0.5:
            self.gscore_reconnect_seconds = 0.5
        if self.gscore_connect_timeout < 1.0:
            self.gscore_connect_timeout = 1.0
        if self.status_interval_seconds < 1.0:
            self.status_interval_seconds = 2.0

    @property
    def admin_ids(self) -> frozenset[str]:
        return frozenset(split_values(self.admin_user_ids))

    @property
    def only_prefixes(self) -> tuple[str, ...]:
        return split_values(self.gscore_only_prefixes)

    @property
    def allowed_channel_types(self) -> frozenset[str]:
        return frozenset(split_values(self.channel_types))

    @property
    def status_label(self) -> str:
        if self.gscore_ws_url:
            return self.gscore_ws_url
        return f"ws://{self.gscore_host}:{self.gscore_port}/ws/{self.gscore_bot_id}"

    def missing_fields(self) -> list[str]:
        if not self.enabled:
            return []
        missing: list[str] = []
        if not self.gscore_host:
            missing.append("gscore_host")
        if not self.gscore_bot_id:
            missing.append("gscore_bot_id")
        if not self.onebot_bot_id:
            missing.append("onebot_bot_id")
        return missing

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        known = {field for field in cls.__dataclass_fields__}
        merged = dict(data)
        # Accept the AstrBot reference plugin's config keys verbatim so users can
        # copy the same values without re-deciding names.
        aliases = {
            "BOT_ID": "gscore_bot_id",
            "IP": "gscore_host",
            "PORT": "gscore_port",
            "WS_TOKEN": "gscore_ws_token",
            "MAX_RETRY_TIMES": "gscore_max_retry",
            "GSCORE_ONLY_PREFIXES": "gscore_only_prefixes",
        }
        for source, target in aliases.items():
            if source in merged and target not in merged:
                merged[target] = merged[source]
        return cls(**{key: value for key, value in merged.items() if key in known})

    @classmethod
    def load(cls, path: Path) -> Self:
        if not path.is_file():
            return cls()
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"invalid connector config {path}: {exc}") from exc
        if not isinstance(raw, dict):
            raise ValueError(f"connector config must be a JSON object: {path}")
        config = cls.from_dict(raw)
        config.apply_env()
        return config

    def apply_env(self) -> None:
        """Optional environment overrides for container-first deployments."""
        env = os.environ
        mappings = {
            "enabled": ("GSCORE_ENABLED", to_bool),
            "gscore_host": ("GSCORE_HOST", str),
            "gscore_port": ("GSCORE_PORT", to_int),
            "gscore_ws_url": ("GSCORE_WS_URL", str),
            "gscore_ws_token": ("GSCORE_WS_TOKEN", str),
            "gscore_bot_id": ("GSCORE_BOT_ID", str),
            "gscore_max_retry": ("GSCORE_MAX_RETRY", to_int),
            "gscore_reconnect_seconds": ("GSCORE_RECONNECT_SECONDS", to_float),
            "onebot_bot_id": ("GSCORE_ONEBOT_BOT_ID", str),
            "qq_group_bot_id": ("GSCORE_QQ_GROUP_BOT_ID", str),
            "feishu_bot_id": ("GSCORE_FEISHU_BOT_ID", str),
            "gscore_only_prefixes": ("GSCORE_ONLY_PREFIXES", str),
            "admin_user_ids": ("GSCORE_ADMIN_USER_IDS", str),
            "channel_types": ("GSCORE_CHANNEL_TYPES", str),
        }
        for field, (env_name, caster) in mappings.items():
            raw = env.get(env_name)
            if raw is None or not str(raw).strip():
                continue
            if caster is str:
                setattr(self, field, raw.strip())
            elif caster is to_bool:
                setattr(self, field, to_bool(raw, getattr(self, field)))
            elif caster is to_int:
                setattr(self, field, to_int(raw, getattr(self, field)))
            else:
                setattr(self, field, to_float(raw, getattr(self, field)))

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_suffix(path.suffix + ".tmp")
        temp.write_text(
            json.dumps(self.to_dict(), ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        temp.replace(path)

    @staticmethod
    def env_signature() -> tuple[tuple[str, str], ...]:
        """Snapshot only GSCORE_* env keys for hot reload."""
        keys = (
            "GSCORE_ENABLED",
            "GSCORE_HOST",
            "GSCORE_PORT",
            "GSCORE_WS_URL",
            "GSCORE_WS_TOKEN",
            "GSCORE_BOT_ID",
            "GSCORE_MAX_RETRY",
            "GSCORE_RECONNECT_SECONDS",
            "GSCORE_ONEBOT_BOT_ID",
            "GSCORE_QQ_GROUP_BOT_ID",
            "GSCORE_FEISHU_BOT_ID",
            "GSCORE_ONLY_PREFIXES",
            "GSCORE_ADMIN_USER_IDS",
            "GSCORE_CHANNEL_TYPES",
        )
        return tuple((key, os.environ.get(key, "")) for key in keys)


__all__ = [
    "ConnectorConfig",
    "default_config_path",
    "default_status_path",
    "split_values",
    "to_bool",
    "to_int",
    "to_float",
]
