"""Bronze writer: idempotent micro-batch upserts into the Iceberg bronze table."""

from pathlib import Path

from pyiceberg.table import Table

from pipeline.common.config import Settings
from pipeline.common.iceberg import (
    BRONZE_TABLE,
    TICKER_TABLE,
    ensure_bronze_table,
    ensure_ticker_table,
    load_lake_catalog,
    ticker_location,
    upsert_events,
    upsert_tickers,
)
from pipeline.common.logging import get_logger
from pipeline.common.models import TickerEvent, TradeEvent
from pipeline.common.retry import with_transient_retry

logger = get_logger("consumer.writer")


class BronzeWriter:
    """Owns the bronze table handle; `write` MERGEs one batch on (symbol, trade_id)."""

    def __init__(self, settings: Settings, warehouse_dir: Path | None = None) -> None:
        catalog = load_lake_catalog(settings, warehouse_dir)
        location = (
            None
            if settings.catalog_type == "sql"
            else f"s3://{settings.s3_bucket}/bronze/{BRONZE_TABLE}"
        )
        self._table: Table = ensure_bronze_table(
            catalog, settings.glue_database, location=location
        )
        logger.info("bronze_ready", table=f"{settings.glue_database}.{BRONZE_TABLE}")

    def write(self, events: list[TradeEvent]) -> int:
        """Append the batch's new rows (partition-scoped anti-join inside).

        Transient S3/Glue/commit failures are retried with backoff; a hard
        failure raises and the caller must not commit offsets (replay appends
        nothing new). Returns rows appended."""
        if not events:
            raise ValueError("refusing to write an empty batch")
        appended = with_transient_retry(
            lambda: upsert_events(self._table, events), action="bronze_write"
        )
        logger.info(
            "bronze_write", rows=len(events), appended=appended, skipped=len(events) - appended
        )
        return appended


class TickerWriter:
    """Owns the bronze ticker handle; `write` MERGEs one batch on (symbol, event_time)."""

    def __init__(self, settings: Settings, warehouse_dir: Path | None = None) -> None:
        catalog = load_lake_catalog(settings, warehouse_dir)
        self._table: Table = ensure_ticker_table(
            catalog, settings.glue_database, location=ticker_location(settings)
        )
        logger.info("ticker_bronze_ready", table=f"{settings.glue_database}.{TICKER_TABLE}")

    def write(self, events: list[TickerEvent]) -> int:
        """Append new snapshots (partition-scoped anti-join); replay appends nothing."""
        if not events:
            raise ValueError("refusing to write an empty batch")
        appended = with_transient_retry(
            lambda: upsert_tickers(self._table, events), action="ticker_bronze_write"
        )
        logger.info(
            "ticker_write",
            rows=len(events),
            appended=appended,
            skipped=len(events) - appended,
        )
        return appended
