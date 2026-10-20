"""Binance trade-stream WebSocket client with reconnect + backoff.

Uses the combined-stream endpoint so all configured symbols ride one connection.
Server ping/pong is handled by the `websockets` library; we run our own keepalive
with a generous timeout (config `ws_ping_timeout_s`) so transient network stalls
do not force a reconnect (Oct 2026: the 20 s default tripped close 1011 repeatedly).
"""

import asyncio
import json
import random
import time
from collections.abc import Callable
from typing import Any

from websockets.asyncio.client import connect

from pipeline.common.config import Settings
from pipeline.common.logging import get_logger

logger = get_logger("producer.ws")

MAX_BACKOFF_S = 30.0
OPEN_TIMEOUT_S = 30.0
CLOSE_TIMEOUT_S = 10.0
THROUGHPUT_WINDOW_S = 30.0


class StaleStreamError(Exception):
    """The socket is open but no market data arrived for too long."""


async def _recv(ws: Any, stale_after_s: float) -> Any:
    """Receive one message, or raise `StaleStreamError` if the stream goes silent.

    Binance keeps answering keepalive pings on streams that have stopped
    delivering data (observed for hours in Oct 2026), so a ping timeout alone is
    not enough to detect a dead subscription.
    """
    try:
        return await asyncio.wait_for(ws.recv(), timeout=stale_after_s)
    except TimeoutError as exc:
        raise StaleStreamError(f"no message for {stale_after_s}s") from exc


def stream_url(settings: Settings) -> str:
    """Combined trade-stream URL for every configured symbol (stream names are lowercase)."""
    streams = "/".join(f"{symbol.lower()}@trade" for symbol in settings.symbols)
    return f"{settings.binance_ws_url}/stream?streams={streams}"


async def stream_trades(
    settings: Settings,
    on_message: Callable[[dict[str, Any]], None],
    stop: asyncio.Event,
) -> None:
    """Connect, invoke `on_message` per trade payload, reconnect forever until `stop`."""
    url = stream_url(settings)
    backoff_s = 1.0
    total = 0
    while not stop.is_set():
        try:
            async with connect(
                url,
                ping_interval=settings.ws_ping_interval_s,
                ping_timeout=settings.ws_ping_timeout_s,
                open_timeout=OPEN_TIMEOUT_S,
                close_timeout=CLOSE_TIMEOUT_S,
            ) as ws:
                logger.info("ws_connected", url=url, symbols=settings.symbols)
                backoff_s = 1.0
                window = 0
                window_start = time.monotonic()
                while not stop.is_set():
                    raw = await _recv(ws, settings.ws_stale_after_s)
                    payload: dict[str, Any] = json.loads(raw)
                    on_message(payload.get("data", payload))
                    total += 1
                    window += 1
                    now = time.monotonic()
                    if now - window_start >= THROUGHPUT_WINDOW_S:
                        logger.info(
                            "ws_throughput",
                            rate_per_s=round(window / (now - window_start), 1),
                            total=total,
                        )
                        window = 0
                        window_start = now
        except StaleStreamError as exc:
            logger.warning(
                "ws_stalled",
                error=str(exc),
                stale_after_s=settings.ws_stale_after_s,
            )
        except Exception as exc:  # noqa: BLE001 — every failure mode must reconnect
            logger.warning("ws_disconnected", error=str(exc))
        if stop.is_set():
            return
        delay = backoff_s + random.uniform(0, 1)  # noqa: S311 — jitter, not crypto
        logger.warning("ws_retry", retry_in_s=round(delay, 1))
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
            return
        except TimeoutError:
            pass
        backoff_s = min(backoff_s * 2, MAX_BACKOFF_S)
