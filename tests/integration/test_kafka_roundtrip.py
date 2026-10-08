"""Live Kafka roundtrip. Skipped unless RUN_KAFKA_TESTS=1 with a reachable broker."""

import os
import time
import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from confluent_kafka import Consumer

from pipeline.common.config import Settings
from pipeline.common.models import TradeEvent
from pipeline.producer.kafka_producer import TradeProducer

pytestmark = pytest.mark.integration

BOOTSTRAP = os.environ.get("TICKSTREAM_KAFKA_BOOTSTRAP", "localhost:9092")


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


def test_produce_consume_roundtrip() -> None:
    if os.environ.get("RUN_KAFKA_TESTS") != "1":
        pytest.skip("needs live Kafka: RUN_KAFKA_TESTS=1")
    topic = f"trades.test.{uuid.uuid4().hex[:8]}"
    settings = Settings(kafka_bootstrap=BOOTSTRAP, topic=topic)
    producer = TradeProducer(settings)
    producer.ensure_topic(partitions=1)
    sent = [_event(trade_id) for trade_id in range(3)]
    for event in sent:
        producer.produce(event)
    assert producer.flush(30) == 0

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"test-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([topic])
    received: list[TradeEvent] = []
    deadline = time.monotonic() + 30
    try:
        while len(received) < len(sent) and time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None or message.error() is not None:
                continue
            value = message.value()
            if value is None:
                continue
            received.append(TradeEvent.model_validate_json(value))
    finally:
        consumer.close()
    assert [e.trade_id for e in received] == [0, 1, 2]
