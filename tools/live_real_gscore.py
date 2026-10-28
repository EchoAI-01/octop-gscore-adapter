"""Real GsCore integration probe.

Starts only a mock NapCat (platform side), waits for the Octop ``gscore``
channel to connect to the real GsCore container, sends the built-in
``gs帮助`` command, and verifies that GsCore's reply is delivered back through
the OneBot transport.

Run inside the Octop container after pointing the test channel at the real
GsCore service.
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path

TOOLS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(TOOLS_DIR))

from mock_napcat import MockNapCat  # noqa: E402

RESULT = Path("/tmp/gscore_real_result.json")
NAPCAT_PORT = 13001


def write_result(payload: dict) -> None:
    payload["updated_at"] = time.time()
    RESULT.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False))


async def main() -> None:
    result = {"success": False, "stage": "starting", "checks": []}
    napcat = MockNapCat(NAPCAT_PORT, host="0.0.0.0")
    try:
        await napcat.start()
        write_result(result)

        async def wait_ws() -> None:
            while napcat.ws is None:
                await asyncio.sleep(0.2)

        await asyncio.wait_for(wait_ws(), timeout=90)
        result["stage"] = "connected"
        result["checks"].append("OneBot channel connected to mock NapCat")
        write_result(result)

        await napcat.send_event(
            {
                "post_type": "message",
                "message_type": "group",
                "message_id": 8801,
                "user_id": 8888,
                "group_id": 6666,
                "message": [
                    {"type": "text", "data": {"text": "core帮助"}},
                ],
                "sender": {"nickname": "RealCore"},
            }
        )
        result["checks"].append("Reported core帮助 to real GsCore")
        write_result(result)

        action = await napcat.wait_action("send_group_msg", timeout=45)
        payload = json.dumps(action["params"], ensure_ascii=False)
        assert "6666" in payload
        assert '"type": "image"' in payload or "image" in payload or "帮助" in payload
        result["checks"].append("Received real GsCore reply via OneBot send_group_msg")
        result["reply_preview"] = payload[:500]
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


if __name__ == "__main__":
    asyncio.run(main())
