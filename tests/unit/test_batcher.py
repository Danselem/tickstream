"""Unit tests for the micro-batch accumulator (no I/O, explicit clock)."""

from datetime import UTC, datetime
from decimal import Decimal
from time import monotonic

import pytest

from pipeline.common.models import TradeEvent
from pipeline.consumer.batcher import TradeBatcher


def _event(trade_id: int) -> TradeEvent:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return TradeEvent(
        symbol="XRPUSDT",
        trade_id=trade_id,
        price=Decimal("0.5"),
        quantity=Decimal("1"),
        event_time=now,
        ingest_time=now,
        is_buyer_maker=False,
    )


def test_flushes_on_size() -> None:
    batcher = TradeBatcher(max_rows=2, interval_s=999)
    batcher.add(_event(1))
    assert not batcher.should_flush(0.0)
    batcher.add(_event(2))
    assert batcher.should_flush(0.0)
    assert [e.trade_id for e in batcher.drain()] == [1, 2]
    assert len(batcher) == 0


def test_flushes_on_interval_only_when_non_empty() -> None:
    batcher = TradeBatcher(max_rows=1000, interval_s=60)
    born = monotonic()
    assert not batcher.should_flush(born + 59.0)  # empty window never flushes
    batcher.add(_event(1))
    assert not batcher.should_flush(born + 59.0)
    assert batcher.should_flush(born + 61.0)


def test_drain_of_empty_window_returns_empty() -> None:
    assert TradeBatcher(max_rows=10, interval_s=60).drain() == []


def test_window_starts_after_flush_not_drain() -> None:
    """Regression (Oct 2026): a flush slower than the interval must not leave the
    next window already expired, which flushed every following batch at 1 row."""
    batcher = TradeBatcher(max_rows=10_000, interval_s=45)
    batcher.start_window(0.0)
    batcher.add(_event(1))
    assert batcher.should_flush(45.0)  # first window elapsed
    batcher.drain()  # simulate a slow write; window must NOT reset here

    # The caller restarts the window only after the flush completes (at t=85).
    batcher.start_window(85.0)
    batcher.add(_event(2))
    assert not batcher.should_flush(86.0)  # fresh window: 1 s is not enough
    assert batcher.should_flush(130.0)  # a full interval later


def test_rejects_invalid_config() -> None:
    with pytest.raises(ValueError, match="max_rows"):
        TradeBatcher(max_rows=0, interval_s=60)
    with pytest.raises(ValueError, match="interval_s"):
        TradeBatcher(max_rows=10, interval_s=0)
