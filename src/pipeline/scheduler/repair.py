"""One-time bronze repairs. Currently: key-level dedup (ADR-015).

Bronze is append-only and normally never rewritten. The Oct 2026 cross-replica
duplication incident required a one-time rewrite to restore the no-duplicate
(symbol, trade_id) invariant so the GX uniqueness gate passes again.

Athena cannot express this rewrite: engine v3 is Trino-based and has no
``INSERT OVERWRITE`` (only ``INSERT INTO`` / ``MERGE`` / ``DELETE``), and neither
MERGE nor DELETE can remove one of two *identical* rows. So we do it in-process
with PyIceberg: scan, keep the earliest-ingested row per key, ``table.overwrite``.
Run with the consumers stopped so the rewrite does not race appends.
"""

from dataclasses import dataclass

import pyarrow as pa
from pyiceberg.table import Table

from pipeline.common.config import Settings
from pipeline.common.iceberg import BRONZE_KEYS, BRONZE_TABLE, load_lake_catalog
from pipeline.common.logging import get_logger

logger = get_logger("scheduler.repair")


@dataclass(frozen=True)
class DedupOutcome:
    """Result of the bronze dedup rewrite."""

    table: str
    rows_before: int
    rows_after: int
    ok: bool
    error: str | None = None

    @property
    def removed(self) -> int:
        return self.rows_before - self.rows_after


def dedup_arrow(arrow: pa.Table) -> pa.Table:
    """Keep exactly one row per (symbol, trade_id): the earliest by ingest_time.

    Uses pandas (already a dependency): stable-sort by ingest_time, then drop
    later rows for a repeated key. Kept as a pure function so it is cheap to test.
    """
    frame = arrow.to_pandas()
    frame = frame.sort_values("ingest_time", kind="stable")
    frame = frame.drop_duplicates(subset=BRONZE_KEYS, keep="first")
    return pa.Table.from_pandas(frame, schema=arrow.schema, preserve_index=False)


def dedup_table(table: Table) -> tuple[int, int]:
    """Dedup an Iceberg table in place. Returns (rows_before, rows_after).

    Only rewrites when duplicates exist, so a clean table produces no new commit.
    """
    arrow = table.scan().to_arrow()
    before = arrow.num_rows
    deduped = dedup_arrow(arrow)
    after = deduped.num_rows
    if after < before:
        table.overwrite(deduped)
    return before, after


def run_dedup(settings: Settings, table_name: str = BRONZE_TABLE) -> DedupOutcome:
    """Run the dedup rewrite once. Returns the outcome; never raises."""
    try:
        catalog = load_lake_catalog(settings)
        table = catalog.load_table(f"{settings.glue_database}.{table_name}")
        before, after = dedup_table(table)
    except Exception as exc:  # noqa: BLE001 — surface as an outcome for the CLI
        logger.warning("dedup_failed", table=table_name, error=str(exc))
        return DedupOutcome(table_name, 0, 0, ok=False, error=str(exc))
    logger.info(
        "dedup_ok",
        table=table_name,
        rows_before=before,
        rows_after=after,
        removed=before - after,
    )
    return DedupOutcome(table_name, before, after, ok=True)
