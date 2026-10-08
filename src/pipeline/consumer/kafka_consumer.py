"""Kafka consumer factory. Offsets are committed manually — only after Iceberg writes."""

from confluent_kafka import Consumer

from pipeline.common.config import Settings


def build_consumer(settings: Settings) -> Consumer:
    """At-least-once consumer: no auto-commit, earliest on first start."""
    return Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": settings.consumer_group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "client.id": "tickstream-consumer",
        }
    )
