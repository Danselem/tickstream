"""Consumer entrypoint: Kafka -> bronze upserts -> commit. `python -m pipeline.consumer`."""

import signal
import time

from pipeline.common.config import get_settings
from pipeline.common.logging import configure_logging, get_logger
from pipeline.common.models import BatchReport, TradeEvent
from pipeline.consumer.batcher import TradeBatcher
from pipeline.consumer.bronze_writer import BronzeWriter
from pipeline.consumer.kafka_consumer import build_consumer
from pipeline.consumer.metrics import consumer_lag
from pipeline.consumer.ops_writer import OpsWriter

logger = get_logger("consumer")


def main() -> None:
    """Poll, micro-batch, upsert, commit. Crash-safe: offsets commit only after writes."""
    settings = get_settings()
    configure_logging(settings.log_level)
    writer = BronzeWriter(settings)
    ops = OpsWriter(settings)
    consumer = build_consumer(settings)
    consumer.subscribe([settings.topic])
    batcher = TradeBatcher(settings.batch_max_rows, float(settings.batch_interval_s))
    logger.info("consumer_started", topic=settings.topic, group=settings.consumer_group)

    running = True

    def _stop(_signum: int, _frame: object) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    def _flush() -> None:
        batch = batcher.drain()
        if not batch:
            return
        started = time.monotonic()
        writer.write(batch)  # raises on failure -> no commit -> replay absorbs it
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
        ops.append_metrics(report)
        logger.info("batch_committed", **report.model_dump(mode="json"))

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
            batcher.add(TradeEvent.model_validate_json(value))
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
