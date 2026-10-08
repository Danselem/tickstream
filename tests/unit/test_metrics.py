"""Unit tests for the lag helper (mocked consumer — no broker)."""

from unittest.mock import MagicMock

from confluent_kafka import TopicPartition

from pipeline.consumer.metrics import consumer_lag


def _consumer() -> MagicMock:
    return MagicMock()


def test_lag_sums_positioned_partitions_only() -> None:
    consumer = _consumer()
    consumer.assignment.return_value = [TopicPartition("t", 0), TopicPartition("t", 1)]
    consumer.get_watermark_offsets.side_effect = [(0, 0), (0, 100)]
    consumer.position.side_effect = [
        [TopicPartition("t", 0, -1001)],  # OFFSET_INVALID: empty partition
        [TopicPartition("t", 1, 90)],
    ]
    assert consumer_lag(consumer) == 10


def test_lag_is_zero_when_unassigned() -> None:
    consumer = _consumer()
    consumer.assignment.return_value = []
    assert consumer_lag(consumer) == 0
