"""Producer serialization tests. No broker needed (produce only buffers locally)."""

from datetime import UTC, datetime
from decimal import Decimal

from pipeline.common.config import Settings
from pipeline.common.models import TradeEvent
from pipeline.producer.kafka_producer import TradeProducer


def _settings() -> Settings:
    return Settings(kafka_bootstrap="localhost:1", topic="trades.test")


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


def test_produce_buffers_keyed_record() -> None:
    producer = TradeProducer(_settings())
    producer.produce(_event())
    assert producer.queued() == 1
    assert producer.poll(0) == 0
