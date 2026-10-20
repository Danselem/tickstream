"""Producer serialization tests. No broker needed (produce only buffers locally)."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from pipeline.common.config import Settings
from pipeline.common.models import TradeEvent
from pipeline.producer.kafka_producer import TradeProducer


def _settings(symbol_partitions: dict[str, int] | None = None) -> Settings:
    return Settings(
        kafka_bootstrap="localhost:1",
        topic="trades.test",
        symbol_partitions=symbol_partitions or {},
    )


def _event() -> TradeEvent:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return TradeEvent(
        symbol="XRPUSDT",
        trade_id=7,
        price=Decimal("0.5"),
        quantity=Decimal("2"),
        event_time=now,
        ingest_time=now,
        is_buyer_maker=True,
    )


class _Recorder:
    def __init__(self) -> None:
        self.kwargs: dict[str, object] = {}

    def produce(self, *args: object, **kwargs: object) -> None:
        self.kwargs = kwargs

    def poll(self, timeout: float) -> int:
        return 0


def test_produce_buffers_keyed_record() -> None:
    producer = TradeProducer(_settings())
    producer.produce(_event())
    assert producer.queued() == 1
    assert producer.poll(0) == 0


def test_configured_symbol_uses_explicit_partition() -> None:
    producer = TradeProducer(_settings({"XRPUSDT": 1}))
    recorder = _Recorder()
    producer._producer = recorder  # type: ignore[assignment]
    producer.produce(_event())
    assert recorder.kwargs["partition"] == 1


def test_unmapped_symbol_falls_back_to_key_hash() -> None:
    producer = TradeProducer(_settings({"BTCUSDT": 0}))
    recorder = _Recorder()
    producer._producer = recorder  # type: ignore[assignment]
    producer.produce(_event())
    assert recorder.kwargs["partition"] == -1


def test_invalid_partition_rejected() -> None:
    with pytest.raises(ValueError, match="outside 0..2"):
        TradeProducer(_settings({"XRPUSDT": 3}))
