"""Octop plugin entry: install the global GsCore connector.

This plugin is deliberately **not** a channel. Octop channels continue to own
their IM connections and feed the Agent as usual; the connector taps every
inbound message before Agent-side group gating, mirrors it to GsCore, and sends
GsCore replies back through the originating channel.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from octop_gateway.channel import BaseChannel

from .config import ConnectorConfig, default_config_path
from .connector import GsCoreConnector, install_global_hooks

logger = logging.getLogger(__name__)

__version__ = "v0.0.1-beta"
PLUGIN_ID = "octop-gscore-adapter"


def _ensure_config(path) -> ConnectorConfig | None:
    try:
        if not path.is_file():
            ConnectorConfig().save(path)
            logger.info(
                "[GsCoreConnector] 已创建配置文件 %s（默认 disabled；配置后自动热加载）",
                path,
            )
        return ConnectorConfig.load(path)
    except Exception as exc:  # noqa: BLE001 - a bad config must not fail plugin load
        logger.error("[GsCoreConnector] 读取/创建配置失败: %s", exc)
        return None


def setup(ctx: Any) -> None:
    """Install the connector; no channel/tool/middleware registration."""
    config_path = default_config_path()
    _ensure_config(config_path)

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:  # plugin loaded outside a running loop
        loop = asyncio.get_event_loop()

    connector = GsCoreConnector(config_path, version=__version__)
    install_global_hooks()

    old = getattr(BaseChannel, "_gscore_connector", None)
    if old is not None and old is not connector:
        loop.create_task(old.stop())

    BaseChannel._gscore_connector = connector  # type: ignore[attr-defined]
    loop.create_task(connector.start())
    logger.info(
        "[GsCoreConnector] plugin %s %s installed (config=%s)",
        PLUGIN_ID,
        __version__,
        config_path,
    )


__all__ = ["PLUGIN_ID", "__version__", "setup"]
