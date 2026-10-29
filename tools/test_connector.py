"""Integration test with fake Octop channels and a mock GsCore."""

from __future__ import annotations

import asyncio
import json
import sys
import tempfile
from pathlib import Path

TOOLS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(TOOLS_DIR))

from _plugin_loader import load_plugin_package  # noqa: E402
from mock_gscore import MockGsCore  # noqa: E402

_pkg = load_plugin_package()
config_mod = __import__("gscore_connector_under_test.config", fromlist=["x"])
connector_mod = __import__("gscore_connector_under_test.connector", fromlist=["x"])

from octop_gateway.channel import BaseChannel  # noqa: E402
from octop_gateway.models import (  # noqa: E402
    ChannelSubject,
    InboundMessage,
    MessageEvent,
    TextContent,
)

ConnectorConfig = config_mod.ConnectorConfig
GsCoreConnector = connector_mod.GsCoreConnector
GsMessage = __import__("gscore_connector_under_test.models", fromlist=["x"]).GsMessage


def text_of(packet: dict) -> str:
    return "".join(
        str(item.get("data"))
        for item in packet.get("content") or []
        if item.get("type") == "text"
    )


def types_of(packet: dict) -> list[str]:
    return [str(item.get("type")) for item in packet.get("content") or []]


class FakeChannel(BaseChannel):
    channel_type = "fake"

    def __init__(self, processor, channel_id: str, channel_type: str = "fake") -> None:
        super().__init__(processor, channel_id=channel_id)
        self.channel_type = channel_type
        self.sent: list[tuple[ChannelSubject, list]] = []
        self.connected = True

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    @property
    def is_connected(self) -> bool:
        return self.connected

    def parse_inbound(self, raw_payload):
        if isinstance(raw_payload, InboundMessage):
            return raw_payload
        raise ValueError("fake channel expects InboundMessage")

    async def _send_text(self, subject, text: str) -> None:
        self.sent.append((subject, [TextContent(text=text)]))

    async def _send_content(self, subject, parts) -> None:
        self.sent.append((subject, list(parts)))

    async def _send_media(self, subject, media) -> None:
        self.sent.append((subject, [media]))


class FakeOneBotChannel(FakeChannel):
    def __init__(self, processor, channel_id: str) -> None:
        super().__init__(processor, channel_id, channel_type="onebot11")
        self._self_id = "10001"
        self.segments: list[tuple[ChannelSubject, list[dict]]] = []
        self.forwards: list[tuple[ChannelSubject, list[dict]]] = []
        self.actions: list[tuple[str, dict]] = []

    async def push_segments(self, subject, segments) -> None:
        self.segments.append((subject, list(segments)))

    async def push_forward(self, subject, nodes) -> None:
        self.forwards.append((subject, list(nodes)))

    async def _call_api(self, action, params, **kwargs):
        self.actions.append((action, dict(params)))
        return {"message_id": 777}


def make_inbound(
    channel_id: str,
    *,
    text: str = "触发词",
    group: bool = True,
    mentioned: bool = True,
) -> InboundMessage:
    metadata = {
        "message_id": "m1",
        "message_type": "group" if group else "private",
        "chat_type": "group" if group else "dm",
        "is_group": group,
        "sender_id": "u1",
        "user_id": "u1",
        "sender_name": "Tester",
        "group_id": "g1" if group else "",
        "bot_mentioned": mentioned,
        "role": "member",
    }
    subject = ChannelSubject(
        subject_id="group:g1" if group else "user:u1",
        chat_type="group" if group else "direct",
        metadata=dict(metadata),
    )
    return InboundMessage(
        channel_id=channel_id,
        channel_type="fake",
        channel_subject=subject,
        content=[TextContent(text=text)],
        metadata=metadata,
        channel_session_id="s1",
    )


async def main() -> None:
    core = MockGsCore(route_bot_id="Octop")
    await core.start()
    agent_seen: list[InboundMessage] = []
    blocked_seen: list[InboundMessage] = []

    async def processor(message):
        if message.metadata.get("gscore_block_agent"):
            blocked_seen.append(message)
            return
        agent_seen.append(message)
        yield MessageEvent.completed()

    with tempfile.TemporaryDirectory() as tmp:
        config_path = Path(tmp) / "config.json"
        ConnectorConfig.from_dict(
            {
                "enabled": "true",
                "gscore_host": "127.0.0.1",
                "gscore_port": core.port,
                "gscore_bot_id": "Octop",
                "gscore_reconnect_seconds": "0.5",
                "channel_types": "fake,onebot11",
                "gscore_only_prefixes": "core",
            }
        ).save(config_path)

        connector = GsCoreConnector(config_path, version="test")
        connector_mod.install_global_hooks()
        BaseChannel._gscore_connector = connector
        await connector.start()
        try:
            fake = FakeChannel(processor, "chan-1")
            await fake.handle_inbound(make_inbound("chan-1", text="触发词"))

            report = await core.wait_for(
                lambda p: p.get("bot_id") == "fake"
                and p.get("group_id") == "g1"
                and "触发词" in text_of(p),
                timeout=8,
            )
            assert report["bot_self_id"] == "chan-1"
            assert report["user_id"] == "u1"
            assert report["msg_id"] == "s1"
            assert any(
                item["type"] == "at" and item["data"] == "chan-1"
                for item in report["content"]
            )
            # The normal Octop Agent path still runs.
            assert agent_seen and agent_seen[0].text == "触发词"

            await core.send_packet(
                {
                    "bot_id": "fake",
                    "bot_self_id": "chan-1",
                    "msg_id": "s1",
                    "target_type": "group",
                    "target_id": "g1",
                    "content": [{"type": "text", "data": "GsCore 回复"}],
                    "echo": "echo-1",
                }
            )
            deadline = asyncio.get_running_loop().time() + 5
            while not fake.sent and asyncio.get_running_loop().time() < deadline:
                await asyncio.sleep(0.05)
            assert fake.sent, "fake channel did not receive GsCore reply"
            subject, parts = fake.sent[-1]
            assert subject.chat_type == "group"
            assert subject.metadata["group_id"] == "g1"
            # Reference semantics: MessageSend.msg_id is a session id; the
            # passive anchor is the cached inbound platform message id.
            assert subject.metadata["msg_id"] == "m1"
            assert any(part.text == "GsCore 回复" for part in parts if isinstance(part, TextContent))

            receipt = await core.wait_for(
                lambda p: types_of(p) == ["recall_message_id"], timeout=5
            )
            assert receipt["content"][0]["data"]["echo"] == "echo-1"

            # Reference GSCORE_ONLY_PREFIXES: report to GsCore, block Agent.
            await fake.handle_inbound(make_inbound("chan-1", text="core隐藏指令"))
            blocked_report = await core.wait_for(
                lambda p: p.get("group_id") == "g1"
                and "core隐藏指令" in text_of(p),
                timeout=8,
            )
            assert blocked_report["bot_self_id"] == "chan-1"
            assert blocked_seen and blocked_seen[-1].text == "core隐藏指令"
            assert all(item.text != "core隐藏指令" for item in agent_seen)

            # Meta event path.
            await connector.observe_onebot_notice(
                fake,
                {
                    "notice_type": "group_increase",
                    "group_id": 123,
                    "user_id": 456,
                    "operator_id": 789,
                    "sub_type": "invite",
                },
            )
            meta = await core.wait_for(
                lambda p: types_of(p) == ["meta-user_join_group"], timeout=5
            )
            assert meta["group_id"] == "123"

            # OneBot native segments + control packets.
            onebot = FakeOneBotChannel(processor, "onebot-1")
            await onebot.handle_inbound(make_inbound("onebot-1", text="onebot"))

            await core.send_packet(
                {
                    "bot_id": "onebot",
                    "bot_self_id": "onebot-1",
                    "target_type": "group",
                    "target_id": "g1",
                    "content": [
                        {"type": "reply", "data": "r1"},
                        {"type": "at", "data": "u2"},
                        {"type": "text", "data": "hello"},
                        {
                            "type": "node",
                            "data": [
                                {"type": "text", "data": "node-a"},
                                {"type": "text", "data": "node-b"},
                            ],
                        },
                    ],
                }
            )
            deadline = asyncio.get_running_loop().time() + 5
            while not any(
                action in {"send_group_msg", "send_group_forward_msg"}
                for action, _ in onebot.actions
            ) and asyncio.get_running_loop().time() < deadline:
                await asyncio.sleep(0.05)
            send_actions = [item for item in onebot.actions if item[0] == "send_group_msg"]
            assert send_actions, onebot.actions
            segments = send_actions[-1][1]["message"]
            assert [seg["type"] for seg in segments][:3] == ["reply", "at", "text"]
            forward_actions = [
                item for item in onebot.actions if item[0] == "send_group_forward_msg"
            ]
            assert forward_actions and len(forward_actions[-1][1]["messages"]) == 2

            await core.send_packet(
                {
                    "bot_id": "onebot",
                    "bot_self_id": "onebot-1",
                    "target_type": "group",
                    "target_id": "g1",
                    "content": [
                        {"type": "excute_delete_message", "data": {"message_id": "88"}}
                    ],
                }
            )
            await asyncio.sleep(0.3)
            assert any(action == "delete_msg" for action, _ in onebot.actions)

            await core.send_packet(
                {
                    "bot_id": "onebot",
                    "bot_self_id": "onebot-1",
                    "target_type": "group",
                    "target_id": "g1",
                    "content": [
                        {
                            "type": "excute_ban_user",
                            "data": {"user_id": "u1", "group_id": "g1", "duration": 60},
                        }
                    ],
                }
            )
            await asyncio.sleep(0.3)
            assert any(action == "set_group_ban" for action, _ in onebot.actions)

            print("connector tests: OK")
        finally:
            BaseChannel._gscore_connector = None
            await connector.stop()
            await core.stop()


if __name__ == "__main__":
    asyncio.run(main())
