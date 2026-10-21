"""Consumer entrypoint: Kafka -> bronze upserts -> commit. `python -m pipeline.consumer`."""

import signal
import time

from confluent_kafka import Consumer, TopicPartition

from pipeline.common.config import get_settings
from pipeline.common.logging import configure_logging, get_logger
from pipeline.common.models import BatchReport, TickerEvent, TradeEvent
from pipeline.common.retry import with_transient_retry
from pipeline.consumer.batcher import Batcher
from pipeline.consumer.bronze_writer import BronzeWriter, TickerWriter
from pipeline.consumer.kafka_consumer import build_consumer
from pipeline.consumer.metrics import consumer_lag
from pipeline.consumer.ops_writer import OpsWriter

logger = get_logger("consumer")


def main(stream: str = "trades") -> None:
    """Poll, micro-batch, upsert, commit. Crash-safe: offsets commit only after writes."""
    settings = get_settings()
    configure_logging(settings.log_level)
    ticker = stream == "ticker"
    topic = settings.ticker_topic if ticker else settings.topic
    group = settings.ticker_consumer_group if ticker else settings.consumer_group
    model: type[TradeEvent] | type[TickerEvent] = TickerEvent if ticker else TradeEvent
    # Table bootstrap does S3/Glue I/O: same transient handling as writes.
    writer: BronzeWriter | TickerWriter
    if ticker:
        writer = with_transient_retry(lambda: TickerWriter(settings), action="ticker_bronze_ensure")
    else:
        writer = with_transient_retry(lambda: BronzeWriter(settings), action="bronze_ensure")
    ops = with_transient_retry(lambda: OpsWriter(settings), action="ops_ensure")
    consumer = build_consumer(settings, topic=topic, group=group)
    batcher: Batcher[TradeEvent] | Batcher[TickerEvent] = Batcher(
        settings.batch_max_rows, float(settings.batch_interval_s)
    )

    running = True

    def _stop(_signum: int, _frame: object) -> None:
        nonlocal running
        running = False

    def _flush() -> None:
        batch = batcher.drain()
        if not batch:
            return
        started = time.monotonic()
        writer.write(batch)  # type: ignore[arg-type]
        consumer.commit(asynchronous=False)
        elapsed_ms = (time.monotonic() - started) * 1000
        max_event_time = max(event.event_time for event in batch)
        report = BatchReport(
            rows=len(batch),
            max_event_time=max_event_time,
            lag_messages=consumer_lag(consumer),
            commit_ms=elapsed_ms,
        )
        # Metrics go after the commit: real lag/commit_ms values. A crash in this
        # window loses one metrics row (gap, never a dup); bronze is unaffected.
        # Metrics must never break the data path: a failed append is logged and
        # skipped (observability degrades; the batch stays committed).
        try:
            ops.append_metrics(report)
        except Exception as exc:  # noqa: BLE001 — observability is best-effort
            logger.warning("metrics_append_failed", error=str(exc))
        # Start the next window only AFTER every blocked step (Metrics appends
        # can take tens of seconds under concurrent commits). Otherwise the new
        # window is already expired when polling resumes and the first message
        # flushes alone — the Oct 2026 1-row stall that grew lag without bound.
        batcher.start_window(time.monotonic())
        logger.info("batch_committed", **report.model_dump(mode="json"))

    def _on_revoke(_consumer: Consumer, partitions: list[TopicPartition]) -> None:
        # Commit the in-flight window before the new owner takes these partitions
        # so it replays as little as possible (ADR-015). _flush is idempotent:
        # with an already-empty buffer it is a no-op.
        nonlocal running
        logger.info("partitions_revoked", partitions=[p.partition for p in partitions])
        try:
            _flush()
        except Exception as exc:  # noqa: BLE001 — never write after losing ownership
            logger.error("revoke_flush_failed", error=str(exc))
            running = False

    def _on_lost(_consumer: Consumer, partitions: list[TopicPartition]) -> None:
        # Lost without a clean revoke (e.g. max.poll exceeded). Stop writing
        # immediately: continuing would let a former owner append rows that the
        # new owner also writes (the Oct 2026 one-row duplicate). The process
        # exits; the container restarts and rejoins with a clean assignment.
        nonlocal running
        logger.error("partitions_lost", partitions=[p.partition for p in partitions])
        running = False

    consumer.subscribe([topic], on_revoke=_on_revoke, on_lost=_on_lost)
    logger.info("consumer_started", stream=stream, topic=topic, group=group)

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    while running:
        message = consumer.poll(1.0)
        if message is None:
            if batcher.should_flush(time.monotonic()):
                _flush()
            continue
        if message.error() is not None:
            logger.warning("kafka_message_error", error=str(message.error()))
            continue
        value = message.value()
        if value is None:
            continue
        try:
            batcher.add(model.model_validate_json(value))  # type: ignore[arg-type]
        except ValueError as exc:
            logger.warning("dropping_malformed_event", error=str(exc))
            continue
        if batcher.should_flush(time.monotonic()):
            _flush()

    _flush()  # final partial window
    consumer.close()
    logger.info("consumer_stopped")


if __name__ == "__main__":
    main()
