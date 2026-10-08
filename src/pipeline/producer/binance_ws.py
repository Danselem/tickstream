"""Binance trade-stream WebSocket client with reconnect + backoff.

Uses the combined-stream endpoint so all configured symbols ride one connection.
Server ping/pong is handled by the `websockets` library.
"""

import asyncio
import json
import random
from collections.abc import Callable
from typing import Any

from websockets.asyncio.client import connect

from pipeline.common.config import Settings
from pipeline.common.logging import get_logger

logger = get_logger("producer.ws")

MAX_BACKOFF_S = 30.0


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
    while not stop.is_set():
        try:
            async with connect(url, ping_interval=20, ping_timeout=20) as ws:
                logger.info("ws_connected", url=url, symbols=settings.symbols)
                backoff_s = 1.0
                async for raw in ws:
                    if stop.is_set():
                        return
                    payload: dict[str, Any] = json.loads(raw)
                    on_message(payload.get("data", payload))
        except Exception as exc:  # noqa: BLE001 — every failure mode must reconnect
            delay = backoff_s + random.uniform(0, 1)  # noqa: S311 — jitter, not crypto
            logger.warning("ws_disconnected", error=str(exc), retry_in_s=round(delay, 1))
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
                return
            except TimeoutError:
                pass
            backoff_s = min(backoff_s * 2, MAX_BACKOFF_S)
