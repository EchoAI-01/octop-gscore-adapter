"""Minimal OneBot 11 WebSocket server used by the integration tests."""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from typing import Any

from websockets.asyncio.server import Server, ServerConnection, serve


class MockNapCat:
    def __init__(
        self,
        port: int = 0,
        *,
        token: str = "",
        self_id: int = 10001,
        host: str = "127.0.0.1",
    ) -> None:
        self.port = port
        self.token = token
        self.self_id = self_id
        self.host = host
        self.server: Server | None = None
        self.ws: ServerConnection | None = None
        self.actions: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.sent_events: list[dict[str, Any]] = []
        self.forward_payloads: dict[str, Any] = {}
        self.message_payloads: dict[str, Any] = {}
        self._seq = 9000

    async def start(self) -> None:
        self.server = await serve(self._handler, self.host, self.port, max_size=2**26)
        sockets = getattr(self.server, "sockets", None) or []
        if sockets:
            self.port = int(sockets[0].getsockname()[1])

    async def _handler(self, ws: ServerConnection) -> None:
        self.ws = ws
        try:
            async for raw in ws:
                packet = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
                action = str(packet.get("action") or "")
                params = packet.get("params") if isinstance(packet.get("params"), dict) else {}
                echo = packet.get("echo")
                self.actions.put_nowait({"action": action, "params": params, "echo": echo})
                data: Any = None
                if action == "get_login_info":
                    data = {"user_id": self.self_id, "nickname": "MockBot"}
                elif action.startswith("send_"):
                    self._seq += 1
                    data = {"message_id": self._seq}
                elif action == "get_forward_msg":
                    data = self.forward_payloads.get(str(params.get("id") or ""), {"messages": []})
                elif action == "get_msg":
                    data = self.message_payloads.get(
                        str(params.get("message_id") or ""), {"message": []}
                    )
                await ws.send(
                    json.dumps(
                        {"status": "ok", "retcode": 0, "data": data, "echo": echo},
                        ensure_ascii=False,
                    )
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            pass

    async def send_event(self, event: dict[str, Any]) -> None:
        assert self.ws is not None, "no adapter connected"
        event.setdefault("self_id", self.self_id)
        event.setdefault("time", int(time.time()))
        self.sent_events.append(event)
        await self.ws.send(json.dumps(event, ensure_ascii=False))

    async def wait_action(self, action: str, timeout: float = 5.0) -> dict[str, Any]:
        async def _loop() -> dict[str, Any]:
            while True:
                packet = await self.actions.get()
                if packet["action"] == action:
                    return packet

        return await asyncio.wait_for(_loop(), timeout=timeout)

    async def stop(self) -> None:
        if self.server is not None:
            self.server.close()
            with contextlib.suppress(Exception):
                await self.server.wait_closed()
            self.server = None
        if self.ws is not None:
            with contextlib.suppress(Exception):
                await self.ws.close()
            self.ws = None
