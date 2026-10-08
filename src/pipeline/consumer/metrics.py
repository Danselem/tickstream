"""Consumer observability: lag and batch summaries for logs (ops tables land in Phase 2)."""

from confluent_kafka import Consumer, TopicPartition

from pipeline.common.logging import get_logger

logger = get_logger("consumer.metrics")


def consumer_lag(consumer: Consumer) -> int:
    """Total unconsumed messages across assigned partitions. 0 when unassigned."""
    total = 0
    try:
        assignment: list[TopicPartition] = consumer.assignment()
    except Exception as exc:  # noqa: BLE001 — metrics must never crash the loop
        logger.warning("lag_check_failed", error=str(exc))
        return 0
    for tp in assignment:
        try:
            _, high = consumer.get_watermark_offsets(tp, timeout=5)
            position = consumer.position([tp])
            fetched = position[0].offset
            if fetched < 0 or high < 0:
                # OFFSET_INVALID/BEGINNING/END/STORED: partition assigned but
                # position unknown (e.g. empty partitions) — contributes no lag.
                continue
            total += max(0, high - fetched)
        except Exception as exc:  # noqa: BLE001 — see above
            logger.warning("lag_partition_failed", partition=tp.partition, error=str(exc))
    return total
