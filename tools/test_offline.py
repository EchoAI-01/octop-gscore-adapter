"""Offline unit tests for the GsCore connector (no network required)."""

from __future__ import annotations

import asyncio
import base64
import json
import sys
import tempfile
from pathlib import Path

TOOLS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(TOOLS_DIR))

from _plugin_loader import load_plugin_package  # noqa: E402

_pkg = load_plugin_package()
config_mod = __import__("gscore_connector_under_test.config", fromlist=["x"])
models_mod = __import__("gscore_connector_under_test.models", fromlist=["x"])
client_mod = __import__("gscore_connector_under_test.client", fromlist=["x"])
content_mod = __import__("gscore_connector_under_test.content", fromlist=["x"])
connector_mod = __import__("gscore_connector_under_test.connector", fromlist=["x"])

ConnectorConfig = config_mod.ConnectorConfig
GsMessage = models_mod.GsMessage
MessageReceive = models_mod.MessageReceive
MessageSend = models_mod.MessageSend
build_ws_url = client_mod.build_ws_url


def test_config() -> None:
    cfg = ConnectorConfig.from_dict(
        {
            "enabled": "true",
            "gscore_host": " core ",
            "gscore_port": "9000",
            "gscore_max_retry": "-1",
            "gscore_reconnect_seconds": "2.5",
            "channel_types": "onebot11, qq",
        }
    )
    assert cfg.enabled is True
    assert cfg.gscore_host == "core"
    assert cfg.gscore_port == 9000
    assert cfg.gscore_max_retry == -1
    assert cfg.gscore_reconnect_seconds == 2.5
    assert cfg.allowed_channel_types == frozenset({"onebot11", "qq"})

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "gscore" / "config.json"
        cfg.save(path)
        loaded = ConnectorConfig.load(path)
        assert loaded.gscore_port == 9000
        assert loaded.channel_types == "onebot11, qq"
        payload = json.loads(path.read_text())
        assert payload["enabled"] is True

    import os

    os.environ["GSCORE_ENABLED"] = "true"
    os.environ["GSCORE_HOST"] = "env-host"
    os.environ["GSCORE_PORT"] = "9999"
    try:
        cfg = ConnectorConfig()
        cfg.apply_env()
        assert cfg.enabled is True
        assert cfg.gscore_host == "env-host"
        assert cfg.gscore_port == 9999
    finally:
        for key in ("GSCORE_ENABLED", "GSCORE_HOST", "GSCORE_PORT"):
            os.environ.pop(key, None)


def test_url_building() -> None:
    cfg = ConnectorConfig.from_dict(
        {"gscore_host": "127.0.0.1", "gscore_port": 8765, "gscore_bot_id": "Octop"}
    )
    assert build_ws_url(cfg) == "ws://127.0.0.1:8765/ws/Octop"

    cfg = ConnectorConfig.from_dict(
        {
            "gscore_host": "https://core.example",
            "gscore_port": 443,
            "gscore_ws_token": "a b&c",
            "gscore_bot_id": "Octop",
        }
    )
    url = build_ws_url(cfg)
    assert url.startswith("wss://core.example:443/ws/Octop?token=")
    assert "a%20b%26c" in url

    cfg = ConnectorConfig.from_dict(
        {"gscore_ws_url": "ws://core:8765/ws/{bot_id}", "gscore_bot_id": "MyBot"}
    )
    assert build_ws_url(cfg) == "ws://core:8765/ws/MyBot"

    cfg = ConnectorConfig.from_dict({"gscore_ws_url": "ws://core/ws", "gscore_bot_id": "MyBot"})
    assert build_ws_url(cfg) == "ws://core/ws/MyBot"


def test_wire_models() -> None:
    receive = MessageReceive.from_dict(
        {
            "bot_id": "onebot",
            "bot_self_id": "chan-1",
            "group_id": 888,
            "user_id": 999,
            "sender": {"nickname": "A"},
            "content": [{"type": "text", "data": "hi"}, {"type": "at", "data": "chan-1"}],
        }
    )
    assert receive.group_id == "888"
    assert receive.user_id == "999"
    assert receive.content[1].data == "chan-1"

    send = MessageSend.from_dict(
        {
            "bot_id": "onebot",
            "bot_self_id": "chan-1",
            "target_type": "group",
            "target_id": 888,
            "content": [{"type": "node", "data": [{"type": "text", "data": "n"}]}],
            "echo": "e1",
            "extra": "ignored",
        }
    )
    assert send.target_id == "888"
    assert send.echo == "e1"
    assert send.content and send.content[0].type == "node"


def test_content_conversion() -> None:
    png = base64.b64encode(b"\x89PNG\r\n").decode()
    parts = content_mod.gs_items_to_parts(
        [
            GsMessage("text", "hello"),
            GsMessage("image", "link://https://example.com/a.png"),
            GsMessage("image", f"base64://{png}"),
            GsMessage("record", f"base64://{png}"),
            GsMessage("video", "https://example.com/v.mp4"),
            GsMessage("file", f"a.txt|base64://{png}"),
            GsMessage("at", "ou_123"),
            GsMessage("node", [{"type": "text", "data": "inside"}]),
            GsMessage("buttons", {"x": 1}),
        ],
        allow_at=True,
    )
    kinds = [part.type.value for part in parts]
    assert kinds.count("text") >= 2
    assert "image" in kinds
    assert "audio" in kinds
    assert "video" in kinds
    assert "file" in kinds

    no_at = content_mod.gs_items_to_parts([GsMessage("at", "1")], allow_at=False)
    assert no_at == []

    class FakeChannel:
        async def load_media_bytes(self, part):
            return b"abc", "image/png"

    async def run() -> None:
        from octop_gateway.models import FileContent, ImageContent, TextContent

        channel = FakeChannel()
        assert (await content_mod.part_to_gs(channel, TextContent(text="x"))).data == "x"
        image = await content_mod.part_to_gs(channel, ImageContent(data="YWJj"))
        assert image.type == "image" and image.data == "base64://YWJj"
        image2 = await content_mod.part_to_gs(channel, ImageContent(url="https://x/y.png"))
        assert image2.data == "https://x/y.png"
        file = await content_mod.part_to_gs(channel, FileContent(filename="a.txt", data="YWJj"))
        assert file.data == "a.txt|base64://YWJj"

    asyncio.run(run())


def test_helpers() -> None:
    assert connector_mod._strip_subject("group:888") == "888"
    assert connector_mod._node_preview(
        [GsMessage("text", "a"), GsMessage("image", "x")]
    ).startswith("[合并转发]")
    assert connector_mod._onebot_media_segment("image", "link://https://x/a.png")[
        "data"
    ]["file"] == "https://x/a.png"
    assert connector_mod._onebot_media_segment("image", "YWJj")["data"]["file"] == "base64://YWJj"


def test_hook_idempotent() -> None:
    from octop_gateway.channel import BaseChannel

    before = BaseChannel.handle_inbound
    connector_mod.install_global_hooks()
    first = BaseChannel.handle_inbound
    connector_mod.install_global_hooks()
    second = BaseChannel.handle_inbound
    assert first is second
    assert getattr(BaseChannel, "_gscore_handle_hook", False) is True
    assert before is not first or getattr(before, "__name__", "") == "handle_inbound_with_gscore"


def main() -> None:
    test_config()
    test_url_building()
    test_wire_models()
    test_content_conversion()
    test_helpers()
    test_hook_idempotent()
    print("offline tests: OK")


if __name__ == "__main__":
    main()
