"""Live connector verification with mock GsCore + mock NapCat.

Run this in a container that shares the host network (or in Octop itself if
the mocks start before the Octop process).  It starts:

* mock GsCore on 0.0.0.0:18765
* mock NapCat on 0.0.0.0:13001

Octop's GsCore connector must point at the mock core, and an existing OneBot
11 channel must point at the mock NapCat.  The probe sends a group message
**without @** — the normal Agent path would gate it out, but the connector taps
before that gate and must still report it to GsCore.
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path

TOOLS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(TOOLS_DIR))

from mock_gscore import MockGsCore  # noqa: E402
from mock_napcat import MockNapCat  # noqa: E402

RESULT = Path("/tmp/gscore_connector_live_result.json")
CORE_PORT = 18765
NAPCAT_PORT = 13001


def write_result(payload: dict) -> None:
    payload["updated_at"] = time.time()
    RESULT.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False))


def text_of(packet: dict) -> str:
    return "".join(
        str(item.get("data"))
        for item in packet.get("content") or []
        if item.get("type") == "text"
    )


def types_of(packet: dict) -> list[str]:
    return [str(item.get("type")) for item in packet.get("content") or []]


async def wait_ws(target, timeout: float = 90.0) -> None:
    async def _loop() -> None:
        while target.ws is None:
            await asyncio.sleep(0.2)

    await asyncio.wait_for(_loop(), timeout=timeout)


async def main() -> None:
    result = {"success": False, "stage": "starting", "checks": []}
    core = MockGsCore(CORE_PORT, host="0.0.0.0", route_bot_id="Octop")
    napcat = MockNapCat(NAPCAT_PORT, host="0.0.0.0")
    try:
        await core.start()
        await napcat.start()
        write_result(result)
        await wait_ws(core)
        result["checks"].append("connector connected to mock GsCore")
        write_result(result)
        await wait_ws(napcat)
        result["checks"].append("existing OneBot channel connected to mock NapCat")
        write_result(result)

        # Give the connector's watch loop time to install the notice hook.
        # Use a unique message id: OneBot channels dedupe by account+chat+id,
        # so a fixed probe id would be dropped on a second run.
        await asyncio.sleep(2.5)
        probe_message_id = int(time.time() * 1000)
        await napcat.send_event(
            {
                "post_type": "message",
                "message_type": "group",
                "message_id": probe_message_id,
                "user_id": 8888,
                "group_id": 6666,
                "message": [{"type": "text", "data": {"text": "连接器监听测试"}}],
                "sender": {"nickname": "LiveUser"},
            }
        )
        report = await core.wait_for(
            lambda p: p.get("group_id") == "6666"
            and "连接器监听测试" in text_of(p),
            timeout=15,
        )
        assert report["user_id"] == "8888"
        assert report["msg_id"]  # reference uses the platform session id here
        # This message was not @-mentioned; the report proves the tap runs
        # before the channel's Agent group gate.
        assert not any(item["type"] == "at" for item in report["content"])
        result["checks"].append("non-@ group message reported before Agent gating")
        write_result(result)

        await core.send_packet(
            {
                "bot_id": "onebot",
                "bot_self_id": report["bot_self_id"],
                "msg_id": report["msg_id"],
                "target_type": "group",
                "target_id": "6666",
                "content": [{"type": "text", "data": "GsCore 关键词回复"}],
                "echo": "live-echo",
            }
        )
        action = await napcat.wait_action("send_group_msg", timeout=15)
        assert action["params"]["group_id"] == 6666
        assert "GsCore 关键词回复" in json.dumps(action["params"], ensure_ascii=False)
        result["checks"].append("GsCore reply routed back through the existing OneBot channel")
        receipt = await core.wait_for(
            lambda p: types_of(p) == ["recall_message_id"], timeout=15
        )
        assert receipt["content"][0]["data"]["echo"] == "live-echo"
        assert receipt["content"][0]["data"]["id"] is not None
        result["checks"].append("recall_message_id receipt returned with platform id")
        write_result(result)

        await core.send_packet(
            {
                "bot_id": "onebot",
                "bot_self_id": report["bot_self_id"],
                "target_type": "group",
                "target_id": "6666",
                "content": [
                    {"type": "excute_delete_message", "data": {"message_id": "7777"}}
                ],
            }
        )
        action = await napcat.wait_action("delete_msg", timeout=15)
        assert action["params"]["message_id"] == 7777
        result["checks"].append("excute_delete_message handled")
        write_result(result)

        await core.send_packet(
            {
                "bot_id": "onebot",
                "bot_self_id": report["bot_self_id"],
                "target_type": "group",
                "target_id": "6666",
                "content": [
                    {
                        "type": "excute_ban_user",
                        "data": {"user_id": "8888", "group_id": "6666", "duration": 60},
                    }
                ],
            }
        )
        action = await napcat.wait_action("set_group_ban", timeout=15)
        assert action["params"]["duration"] == 60
        result["checks"].append("excute_ban_user handled")
        write_result(result)

        await napcat.send_event(
            {
                "post_type": "notice",
                "notice_type": "group_increase",
                "group_id": 6666,
                "user_id": 8888,
                "operator_id": 10001,
                "sub_type": "invite",
            }
        )
        meta = await core.wait_for(
            lambda p: types_of(p) == ["meta-user_join_group"], timeout=15
        )
        assert meta["content"][0]["data"]["user_id"] == "8888"
        result["checks"].append("OneBot notice hook reported user_join_group meta")
        result["success"] = True
        result["stage"] = "done"
        write_result(result)
    except Exception as exc:
        result["stage"] = "failed"
        result["error"] = f"{type(exc).__name__}: {exc}"
        write_result(result)
        raise
    finally:
        await napcat.stop()
        await core.stop()


if __name__ == "__main__":
    asyncio.run(main())
