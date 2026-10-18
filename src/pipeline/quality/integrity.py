"""Whole-table bronze integrity check (ADR-016): count duplicate keys over all rows.

The windowed GX uniqueness gate (ADR-002) is cheap and frequent but only sees the
recent window; a duplicate that falls outside it would hide. This coarse check
scans only the key columns of the whole table (local PyIceberg, no Athena cost)
and records the duplicate count in the `integrity_results` ops table. The
scheduler runs it on the maintenance cadence and blocks dbt when it is nonzero.
"""

import time
from datetime import UTC, datetime
from pathlib import Path

from pyiceberg.table import Table

from pipeline.common.config import Settings
from pipeline.common.iceberg import (
    BRONZE_KEYS,
    BRONZE_TABLE,
    append_arrow,
    bronze_location,
    ensure_bronze_table,
    ensure_ops_tables,
    integrity_result_to_arrow,
    load_lake_catalog,
)
from pipeline.common.logging import get_logger
from pipeline.common.models import IntegrityResult

logger = get_logger("quality.integrity")


def check_integrity(bronze: Table, table_name: str = BRONZE_TABLE) -> IntegrityResult:
    """Count duplicate (symbol, trade_id) keys across the whole table.

    Reads only the key columns; `duplicates = total_rows - distinct_keys`.
    """
    started = time.monotonic()
    keys = bronze.scan(selected_fields=tuple(BRONZE_KEYS)).to_arrow()
    total = keys.num_rows
    frame = keys.to_pandas()
    distinct = int(frame.drop_duplicates(subset=BRONZE_KEYS).shape[0]) if total else 0
    result = IntegrityResult(
        checked_at=datetime.now(UTC),
        table_name=table_name,
        total_rows=total,
        distinct_keys=distinct,
        duplicates=total - distinct,
        run_ms=(time.monotonic() - started) * 1000,
    )
    logger.info("integrity_checked", ok=result.ok, **result.model_dump(mode="json"))
    return result


def run_integrity(settings: Settings, warehouse_dir: Path | None = None) -> IntegrityResult:
    """Check the bronze table and append one row to `integrity_results`."""
    catalog = load_lake_catalog(settings, warehouse_dir)
    bronze = ensure_bronze_table(
        catalog, settings.glue_database, location=bronze_location(settings)
    )
    ops = ensure_ops_tables(catalog, settings.glue_database)
    result = check_integrity(bronze)
    append_arrow(ops.integrity, integrity_result_to_arrow(result))
    return result
