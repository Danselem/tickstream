"""PyIceberg catalog access + bronze table bootstrap.

Glue catalog in production, SQL catalog (sqlite + local dir) for offline pytest.
Credentials always come from the standard AWS env chain — never from code.
"""

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table, UpsertResult
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import (
    BooleanType,
    DecimalType,
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

from pipeline.common.config import Settings
from pipeline.common.models import BatchReport, QualityResult, TradeEvent

BRONZE_TABLE = "bronze_trades"
JOIN_COLS = ["symbol", "trade_id"]

# Ops tables live in the same Glue database (ADR-008): unpartitioned append
# tables. Tiny by design (one row per batch / per quality run); Superset reads
# them via Athena in Phase 3.
QUALITY_RESULTS_TABLE = "quality_results"
PIPELINE_METRICS_TABLE = "pipeline_metrics"


@dataclass(frozen=True)
class OpsTables:
    """Handles for the two ops tables (named fields — never rely on order)."""

    quality: Table
    metrics: Table

BRONZE_ARROW_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("trade_id", pa.int64(), nullable=False),
        pa.field("price", pa.decimal128(20, 8), nullable=False),
        pa.field("quantity", pa.decimal128(20, 8), nullable=False),
        pa.field("event_time", pa.timestamp("us"), nullable=False),
        pa.field("ingest_time", pa.timestamp("us"), nullable=False),
        pa.field("is_buyer_maker", pa.bool_(), nullable=False),
    ]
)


def load_lake_catalog(settings: Settings, warehouse_dir: Path | None = None) -> Catalog:
    """Build the Iceberg catalog. `warehouse_dir` is required for the offline SQL catalog."""
    if settings.catalog_type == "sql":
        if warehouse_dir is None:
            raise ValueError("warehouse_dir is required when catalog_type='sql'")
        return load_catalog(
            "offline",
            **{
                "type": "sql",
                "uri": f"sqlite:///{warehouse_dir}/catalog.db",
                "warehouse": f"file://{warehouse_dir}/warehouse",
            },
        )
    return load_catalog(
        "glue",
        **{
            "type": "glue",
            "glue.region": settings.aws_region,
            "s3.region": settings.aws_region,
            "warehouse": f"s3://{settings.s3_bucket}/warehouse",
        },
    )


def bronze_schema() -> Schema:
    """Bronze schema: raw trade facts, UTC timestamps, decimal money fields."""
    return Schema(
        NestedField(1, "symbol", StringType(), required=True),
        NestedField(2, "trade_id", LongType(), required=True),
        NestedField(3, "price", DecimalType(20, 8), required=True),
        NestedField(4, "quantity", DecimalType(20, 8), required=True),
        NestedField(5, "event_time", TimestampType(), required=True),
        NestedField(6, "ingest_time", TimestampType(), required=True),
        NestedField(7, "is_buyer_maker", BooleanType(), required=True),
    )


def bronze_partition_spec() -> PartitionSpec:
    """Partition by event day + symbol (Brief: date and symbol)."""
    return PartitionSpec(
        PartitionField(source_id=5, field_id=1000, transform=DayTransform(), name="event_date"),
        PartitionField(source_id=1, field_id=1001, transform=IdentityTransform(), name="symbol"),
    )


def bronze_location(settings: Settings) -> str | None:
    """Explicit S3 prefix for bronze in production; None lets the SQL catalog default."""
    if settings.catalog_type == "sql":
        return None
    return f"s3://{settings.s3_bucket}/bronze/{BRONZE_TABLE}"


def ensure_bronze_table(
    catalog: Catalog, database: str, location: str | None = None
) -> Table:
    """Create the bronze table if absent (idempotent). Returns the loaded table."""
    return catalog.create_table_if_not_exists(
        f"{database}.{BRONZE_TABLE}",
        schema=bronze_schema(),
        partition_spec=bronze_partition_spec(),
        location=location,
        properties={"format-version": "2", "write.format.default": "parquet"},
    )


def events_to_arrow(events: list[TradeEvent]) -> pa.Table:
    """Convert validated events to Arrow. Timestamps stored as naive UTC (Iceberg contract)."""
    rows = [
        {
            "symbol": event.symbol,
            "trade_id": event.trade_id,
            "price": event.price,
            "quantity": event.quantity,
            "event_time": event.event_time.astimezone(UTC).replace(tzinfo=None),
            "ingest_time": event.ingest_time.astimezone(UTC).replace(tzinfo=None),
            "is_buyer_maker": event.is_buyer_maker,
        }
        for event in events
    ]
    return pa.Table.from_pylist(rows, schema=BRONZE_ARROW_SCHEMA)


def upsert_events(table: Table, events: list[TradeEvent]) -> UpsertResult:
    """Idempotent micro-batch write: MERGE on (symbol, trade_id). Replays are safe."""
    return table.upsert(events_to_arrow(events), join_cols=JOIN_COLS)


QUALITY_RESULTS_ARROW_SCHEMA = pa.schema(
    [
        pa.field("check_at", pa.timestamp("us"), nullable=False),
        pa.field("success", pa.bool_(), nullable=False),
        pa.field("window_start", pa.timestamp("us"), nullable=False),
        pa.field("window_end", pa.timestamp("us"), nullable=False),
        pa.field("rows_scanned", pa.int64(), nullable=False),
        pa.field("failed_expectations", pa.string(), nullable=False),
        pa.field("run_ms", pa.float64(), nullable=False),
    ]
)

PIPELINE_METRICS_ARROW_SCHEMA = pa.schema(
    [
        pa.field("collected_at", pa.timestamp("us"), nullable=False),
        pa.field("rows", pa.int64(), nullable=False),
        pa.field("max_event_time", pa.timestamp("us"), nullable=True),
        pa.field("lag_messages", pa.int64(), nullable=False),
        pa.field("commit_ms", pa.float64(), nullable=False),
    ]
)


def quality_results_schema() -> Schema:
    """Ops table: one row per GX run (append-only; replays may duplicate rows)."""
    return Schema(
        NestedField(1, "check_at", TimestampType(), required=True),
        NestedField(2, "success", BooleanType(), required=True),
        NestedField(3, "window_start", TimestampType(), required=True),
        NestedField(4, "window_end", TimestampType(), required=True),
        NestedField(5, "rows_scanned", LongType(), required=True),
        NestedField(6, "failed_expectations", StringType(), required=True),
        NestedField(7, "run_ms", DoubleType(), required=True),
    )


def pipeline_metrics_schema() -> Schema:
    """Ops table: one row per consumer micro-batch (append-only)."""
    return Schema(
        NestedField(1, "collected_at", TimestampType(), required=True),
        NestedField(2, "rows", LongType(), required=True),
        NestedField(3, "max_event_time", TimestampType(), required=False),
        NestedField(4, "lag_messages", LongType(), required=True),
        NestedField(5, "commit_ms", DoubleType(), required=True),
    )


def _naive_utc(value: datetime) -> datetime:
    """Iceberg timestamps are naive UTC; strip tzinfo on the way in."""
    return value.astimezone(UTC).replace(tzinfo=None)


def quality_result_to_arrow(result: QualityResult) -> pa.Table:
    """Convert a QualityResult to Arrow. failed_expectations stored as JSON."""
    return pa.Table.from_pylist(
        [
            {
                "check_at": _naive_utc(result.check_at),
                "success": result.success,
                "window_start": _naive_utc(result.window_start),
                "window_end": _naive_utc(result.window_end),
                "rows_scanned": result.rows_scanned,
                "failed_expectations": json.dumps(list(result.failed_expectations)),
                "run_ms": result.run_ms,
            }
        ],
        schema=QUALITY_RESULTS_ARROW_SCHEMA,
    )


def metrics_to_arrow(report: BatchReport, collected_at: datetime) -> pa.Table:
    """Convert a BatchReport to Arrow for the pipeline_metrics ops table."""
    max_event_time = (
        None if report.max_event_time is None else _naive_utc(report.max_event_time)
    )
    return pa.Table.from_pylist(
        [
            {
                "collected_at": _naive_utc(collected_at),
                "rows": report.rows,
                "max_event_time": max_event_time,
                "lag_messages": report.lag_messages,
                "commit_ms": report.commit_ms,
            }
        ],
        schema=PIPELINE_METRICS_ARROW_SCHEMA,
    )


def ensure_ops_tables(catalog: Catalog, database: str) -> "OpsTables":
    """Create quality_results + pipeline_metrics if absent (idempotent)."""
    properties = {"format-version": "2", "write.format.default": "parquet"}
    quality = catalog.create_table_if_not_exists(
        f"{database}.{QUALITY_RESULTS_TABLE}",
        schema=quality_results_schema(),
        properties=properties,
    )
    metrics = catalog.create_table_if_not_exists(
        f"{database}.{PIPELINE_METRICS_TABLE}",
        schema=pipeline_metrics_schema(),
        properties=properties,
    )
    return OpsTables(quality=quality, metrics=metrics)


def append_arrow(table: Table, df: pa.Table) -> None:
    """Append-only write for ops tables (no dedup key by design)."""
    table.append(df)
