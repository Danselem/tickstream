"""Idempotent Kafka producer wrapper. Delivery failures are logged, never raised."""

from confluent_kafka import KafkaError, Message, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from pipeline.common.config import Settings
from pipeline.common.logging import get_logger
from pipeline.common.models import TickerEvent, TradeEvent

logger = get_logger("producer.kafka")

TRADE_PARTITIONS = 3


class EventProducer:
    """Produces validated events, keyed by symbol, into one topic."""

    def __init__(
        self,
        settings: Settings,
        *,
        topic: str,
        partition_map: dict[str, int] | None = None,
    ) -> None:
        self._topic = topic
        self._bootstrap = settings.kafka_bootstrap
        self._partition_map = dict(partition_map or {})
        for symbol, partition in self._partition_map.items():
            if not 0 <= partition < TRADE_PARTITIONS:
                raise ValueError(
                    f"partition {partition} for {symbol} is outside 0..{TRADE_PARTITIONS - 1}"
                )
        self._producer = Producer(
            {
                "bootstrap.servers": self._bootstrap,
                "acks": "all",
                "enable.idempotence": True,
                "compression.codec": "zstd",
                "client.id": "tickstream-producer",
            }
        )

    def ensure_topic(
        self, *, partitions: int = TRADE_PARTITIONS, retention_ms: int = 7 * 24 * 3600 * 1000
    ) -> None:
        """Create the topic if absent (replication 1: single KRaft broker)."""
        admin = AdminClient({"bootstrap.servers": self._bootstrap})
        futures = admin.create_topics(
            [
                NewTopic(
                    self._topic,
                    num_partitions=partitions,
                    replication_factor=1,
                    config={"retention.ms": str(retention_ms)},
                )
            ]
        )
        for name, future in futures.items():
            try:
                future.result(timeout=30)
                logger.info("topic_created", topic=name)
            except Exception as exc:  # noqa: BLE001 — TopicExists is the expected path
                if "TOPIC_ALREADY_EXISTS" in str(exc) or "already exists" in str(exc).lower():
                    logger.info("topic_exists", topic=name)
                else:
                    raise

    def produce(self, event: TradeEvent | TickerEvent) -> None:
        """Queue one event. Call `poll`/`flush` to drive delivery callbacks."""
        self._producer.produce(
            self._topic,
            key=event.symbol.encode(),
            value=event.model_dump_json().encode(),
            partition=self._partition_map.get(event.symbol, -1),
            on_delivery=_on_delivery,
        )
        self._producer.poll(0)

    def poll(self, timeout_s: float) -> int:
        """Drive callbacks; returns number of events served."""
        return self._producer.poll(timeout_s)

    def flush(self, timeout_s: float = 30.0) -> int:
        """Block until all queued messages are delivered. Returns undelivered count."""
        return self._producer.flush(timeout_s)

    def queued(self) -> int:
        """Messages currently buffered client-side."""
        return len(self._producer)


class TradeProducer(EventProducer):
    """Trade producer: topic + optional per-symbol partition pinning (ADR-028)."""

    def __init__(self, settings: Settings) -> None:
        super().__init__(
            settings,
            topic=settings.topic,
            partition_map=settings.symbol_partitions,
        )


class TickerProducer(EventProducer):
    """Ticker producer: hash partitioning (1 Hz per symbol, no skew risk)."""

    def __init__(self, settings: Settings) -> None:
        super().__init__(settings, topic=settings.ticker_topic)


def _on_delivery(error: KafkaError | None, message: Message) -> None:
    if error is not None:
        logger.error(
            "produce_failed",
            topic=message.topic(),
            partition=message.partition(),
            error=str(error),
        )
