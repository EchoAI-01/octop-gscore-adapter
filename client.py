"""GsCore WebSocket client.

Protocol contract (see gsuid-core ``.agents/skills/gscore-adapter-development``):
- one WebSocket per adapter/route ``bot_id``, URL ``/ws/{bot_id}``;
- every frame is **binary** JSON (core calls ``receive_bytes`` / ``send_bytes``);
- reports are serialized through one queue, outbound packets are dispatched to
  the channel callback, one bad packet never kills the receive loop;
- reconnects with backoff and reuses the same route ``bot_id`` so core can keep
  its five-minute outbound queue.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Awaitable, Callable
from urllib.parse import quote, urlparse

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed
from websockets.protocol import State

from .config import ConnectorConfig
from .models import MessageReceive, MessageSend

logger = logging.getLogger(__name__)

SendHandler = Callable[[MessageSend], Awaitable[None]]
MAX_FRAME_BYTES = 2**26  # 64 MiB: base64 images/videos must fit.
MAX_QUEUE_ITEMS = 5000


class GsCoreClient:
    """Maintain the GsCore connection, report queue and reconnect loop."""

    def __init__(self, config: ConnectorConfig, on_send: SendHandler) -> None:
        self._config = config
        self._on_send = on_send
        self._route_bot_id = config.gscore_bot_id or "Octop"
        self._ws: ClientConnection | None = None
        self._queue: asyncio.Queue[MessageReceive] = asyncio.Queue()
        self._supervisor: asyncio.Task[None] | None = None
        self._running = False
        self._runtime_error: str | None = "未启动"
        self._connected_once = False

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------
    @property
    def route_bot_id(self) -> str:
        return self._route_bot_id

    @property
    def is_running(self) -> bool:
        return self._supervisor is not None and not self._supervisor.done()

    @property
    def is_connected(self) -> bool:
        ws = self._ws
        if ws is None:
            return False
        try:
            return ws.state is State.OPEN
        except (AttributeError, RuntimeError):
            return not getattr(ws, "closed", True)

    @property
    def runtime_error(self) -> str | None:
        if self.is_connected:
            return None
        return self._runtime_error

    @property
    def connect_url(self) -> str:
        return build_ws_url(self._config)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self) -> None:
        if self.is_running:
            return
        self._running = True
        self._runtime_error = "连接中"
        self._supervisor = asyncio.create_task(
            self._run(), name=f"gscore-client-{self._route_bot_id}"
        )

    async def stop(self) -> None:
        self._running = False
        task, self._supervisor = self._supervisor, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        ws, self._ws = self._ws, None
        if ws is not None:
            with contextlib.suppress(Exception):
                await ws.close()
        self._runtime_error = "已停止"

    async def report(self, msg: MessageReceive) -> None:
        """Queue one report; the queue survives temporary disconnects."""
        if not self._running:
            logger.warning("[GsCore] client not running, dropping report for %s", msg.bot_id)
            return
        if self._queue.qsize() >= MAX_QUEUE_ITEMS:
            # Keep the newest traffic instead of growing without bound while
            # core is down for a long time.
            with contextlib.suppress(asyncio.QueueEmpty):
                dropped = self._queue.get_nowait()
                logger.warning(
                    "[GsCore] report queue full (%d), dropping oldest %s message",
                    MAX_QUEUE_ITEMS,
                    dropped.bot_id,
                )
        await self._queue.put(msg)

    # ------------------------------------------------------------------
    # Connection / reconnect
    # ------------------------------------------------------------------
    async def _run(self) -> None:
        retry = 0
        cfg = self._config
        while self._running:
            try:
                ws = await self._connect()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - connection errors are expected
                retry += 1
                self._runtime_error = f"连接 GsCore 失败: {_short_error(exc)}"
                if cfg.gscore_max_retry != -1 and retry > cfg.gscore_max_retry:
                    logger.error(
                        "[GsCore] 已达到最大重试次数 (%s)，停止重连: %s",
                        cfg.gscore_max_retry,
                        self.connect_url,
                    )
                    self._runtime_error = (
                        f"已达到最大重试次数 {cfg.gscore_max_retry}，"
                        f"无法连接 {self.connect_url}"
                    )
                    break
                logger.warning(
                    "[GsCore] 连接失败: %s（第 %s 次，%.1fs 后重试）",
                    exc,
                    retry,
                    cfg.gscore_reconnect_seconds,
                )
                await asyncio.sleep(cfg.gscore_reconnect_seconds)
                continue

            retry = 0
            self._connected_once = True
            self._runtime_error = None
            try:
                await self._serve(ws)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - never kill the supervisor
                logger.exception("[GsCore] receive loop crashed")
            finally:
                self._ws = None
            if not self._running:
                break
            self._runtime_error = "与 GsCore 的连接已断开，等待重连"
            logger.warning("[GsCore] 与 GsCore 断开，%.1fs 后重连", cfg.gscore_reconnect_seconds)
            await asyncio.sleep(cfg.gscore_reconnect_seconds)

    async def _connect(self) -> ClientConnection:
        url = self.connect_url
        logger.info("[GsCore] 连接 %s ...", _redact_url(url))
        ws = await connect(
            url,
            max_size=MAX_FRAME_BYTES,
            open_timeout=self._config.gscore_connect_timeout,
            ping_interval=20,
            ping_timeout=30,
            close_timeout=5,
        )
        self._ws = ws
        logger.info("[GsCore] 已连接 %s", _redact_url(url))
        return ws

    async def _serve(self, ws: ClientConnection) -> None:
        send_task = asyncio.create_task(
            self._send_loop(ws), name=f"gscore-send-{self._route_bot_id}"
        )
        try:
            async for raw in ws:
                try:
                    await self._handle_packet(raw)
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001 - one bad packet only
                    logger.exception("[GsCore] 处理下发数据包失败")
        except ConnectionClosed:
            pass
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - socket-level failures
            logger.debug("[GsCore] WebSocket 读取结束", exc_info=True)
        finally:
            send_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await send_task

    async def _send_loop(self, ws: ClientConnection) -> None:
        while True:
            msg = await self._queue.get()
            try:
                await ws.send(_encode(msg))
            except asyncio.CancelledError:
                raise
            except Exception:
                # Put it back so the next connection flushes it first.
                self._queue.put_nowait(msg)
                logger.debug("[GsCore] 上报发送失败，消息已放回队列", exc_info=True)
                return

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------
    async def _handle_packet(self, raw: str | bytes) -> None:
        if isinstance(raw, (bytes, bytearray, memoryview)):
            text = bytes(raw).decode("utf-8")
        else:
            text = raw
        payload = json.loads(text)
        if not isinstance(payload, dict):
            logger.warning("[GsCore] 下发数据包不是 JSON 对象: %s", type(payload))
            return

        msg = MessageSend.from_dict(payload)

        # Core -> adapter log echo.  ``bot_id`` equals the *route* id here, not
        # a platform id.  Never send these to a chat.
        if msg.bot_id == self._route_bot_id:
            first = (msg.content or [None])[0]
            if first is not None and isinstance(first.type, str) and first.type.startswith("log"):
                level = first.type.split("_")[-1].lower()
                log_fn = getattr(logger, level, logger.info)
                log_fn("[GsCore] %s", first.data)
                return

        await self._on_send(msg)


def _encode(msg: MessageReceive) -> bytes:
    # Binary frame, matching core's ``websocket.receive_bytes()``.
    return json.dumps(msg.to_dict(), ensure_ascii=False, separators=(",", ":")).encode(
        "utf-8"
    )


def build_ws_url(config: ConnectorConfig) -> str:
    """Build the adapter URL, accepting Dashboard-friendly overrides.

    ``gscore_ws_url`` semantics:
    - contains ``{bot_id}`` -> formatted;
    - ends with ``/ws`` -> append the route bot id;
    - otherwise it is used verbatim (advanced: reverse proxy / custom path).
    """
    route_bot_id = quote(config.gscore_bot_id or "Octop", safe="")
    override = (config.gscore_ws_url or "").strip()
    if override:
        if "{bot_id}" in override:
            url = override.replace("{bot_id}", route_bot_id)
        elif override.rstrip("/").endswith("/ws"):
            url = f"{override.rstrip('/')}/{route_bot_id}"
        else:
            url = override
    else:
        host = (config.gscore_host or "localhost").strip()
        scheme = "ws"
        if "://" in host:
            raw_scheme, host = host.split("://", 1)
            scheme = "wss" if raw_scheme.lower() in {"https", "wss"} else "ws"
        host = host.strip("/")
        parsed = urlparse(f"{scheme}://{host}")
        if parsed.port is None and config.gscore_port:
            host = f"{host}:{config.gscore_port}"
        url = f"{scheme}://{host}/ws/{route_bot_id}"

    token = config.gscore_ws_token
    if token:
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}token={quote(token, safe='')}"
    return url


def _redact_url(url: str) -> str:
    if "token=" not in url:
        return url
    base, _, _ = url.partition("token=")
    return f"{base}token=***"


def _short_error(exc: BaseException, limit: int = 240) -> str:
    text = str(exc).strip() or exc.__class__.__name__
    return text if len(text) <= limit else text[:limit] + "..."


__all__ = ["GsCoreClient", "build_ws_url", "MAX_FRAME_BYTES", "MAX_QUEUE_ITEMS"]
