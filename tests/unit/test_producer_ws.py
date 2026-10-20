import asyncio

import pytest

from pipeline.producer.binance_ws import StaleStreamError, _recv


class _Silent:
    async def recv(self) -> str:
        await asyncio.sleep(10)
        return ""


class _OneMessage:
    async def recv(self) -> str:
        return "payload"


def test_recv_returns_message() -> None:
    assert asyncio.run(_recv(_OneMessage(), 1)) == "payload"


def test_recv_raises_when_stream_is_silent() -> None:
    with pytest.raises(StaleStreamError, match="no message for 0.01s"):
        asyncio.run(_recv(_Silent(), 0.01))
