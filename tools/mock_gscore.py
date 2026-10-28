"""A minimal GsCore-compatible WebSocket server for integration tests.

It implements only the wire contract this adapter needs: binary JSON frames,
``/ws/{bot_id}`` routing, optional ``?token=`` authentication, and manual
``send_packet()`` so tests can drive every outbound branch.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from typing import Any
from urllib.parse import parse_qs, urlparse

from websockets.asyncio.server import Server, ServerConnection, serve


class MockGsCore:
    def __init__(
        self,
        port: int = 0,
        *,
        token: str = "",
        route_bot_id: str = "Octop",
        host: str = "127.0.0.1",
    ) -> None:
        self.port = port
        self.token = token
        self.route_bot_id = route_bot_id
        self.host = host
        self.server: Server | None = None
        self.ws: ServerConnection | None = None
        self.received: list[dict[str, Any]] = []
        self.queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.connections = 0

    async def start(self) -> None:
        self.server = await serve(
            self._handler,
            self.host,
            self.port,
            max_size=2**26,
            process_request=self._process_request,
        )
        sockets = getattr(self.server, "sockets", None) or []
        if sockets:
            self.port = int(sockets[0].getsockname()[1])

    def _process_request(self, connection: ServerConnection, request: Any) -> Any:
        path = urlparse(request.path)
        if path.path.rstrip("/") != f"/ws/{self.route_bot_id}".rstrip("/"):
            return connection.respond(404, "Not Found")
        if self.token:
            supplied = (parse_qs(path.query).get("token") or [""])[0]
            if supplied != self.token:
                return connection.respond(401, "Unauthorized")
        return None

    async def _handler(self, ws: ServerConnection) -> None:
        self.ws = ws
        self.connections += 1
        try:
            async for raw in ws:
                if not isinstance(raw, (bytes, bytearray)):
                    raise AssertionError(f"GsCore requires binary frames, got {type(raw)}")
                packet = json.loads(bytes(raw).decode("utf-8"))
                self.received.append(packet)
                await self.queue.put(packet)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass

    async def send_packet(self, packet: dict[str, Any]) -> None:
        assert self.ws is not None, "no adapter connected"
        await self.ws.send(
            json.dumps(packet, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        )

    async def wait_for(
        self,
        predicate,
        timeout: float = 5.0,
    ) -> dict[str, Any]:
        async def _loop() -> dict[str, Any]:
            while True:
                packet = await self.queue.get()
                if predicate(packet):
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
