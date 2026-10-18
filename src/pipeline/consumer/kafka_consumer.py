"""Kafka consumer factory. Offsets are committed manually — only after Iceberg writes."""

from confluent_kafka import Consumer

from pipeline.common.config import Settings


def build_consumer(settings: Settings) -> Consumer:
    """At-least-once consumer: no auto-commit, earliest on first start.

    max.poll.interval.ms is 10 min (not the 5 min default): big backlog batches +
    S3 retry storms must fit inside the poll window or the group thrashes (Oct 2026).

    cooperative-sticky (ADR-015): with 3 replicas the group rebalances on deploys
    and scale changes. The cooperative protocol keeps a replica's non-revoked
    partitions during a reassignment, so a waking consumer replays fewer records.
    The consumer also flushes + commits on revoke (see consumer.run) so the old
    owner's in-flight window is committed before the new owner takes over.
    """
    return Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": settings.consumer_group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "max.poll.interval.ms": 600_000,
            "partition.assignment.strategy": "cooperative-sticky",
            "client.id": "tickstream-consumer",
        }
    )
