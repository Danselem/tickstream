"""One-time bronze repairs. Currently: key-level dedup (ADR-015).

Bronze is append-only and normally never rewritten. The Oct 2026 cross-replica
duplication incident required a one-time rewrite to restore the no-duplicate
(symbol, trade_id) invariant so the GX uniqueness gate passes again.

Athena cannot remove *one* of two identical rows in a single statement (Trino has
no ``INSERT OVERWRITE``), but it can do it in two: ``DELETE`` both copies by key,
then ``INSERT`` one back. That runs server-side and prunes by partition/stats, so
it is the fast path for a handful of keys on the current 4k-file layout (the
Oct 2026 repair took ~40 s; a PyIceberg day-scoped rewrite did not finish in
15 min). The in-process PyIceberg path (`dedup_table`/`dedup_key`) is kept for
small/compacted tables and offline tests: scan, keep the earliest-ingested row
per key, ``table.overwrite``. Run either with the consumers stopped so the
rewrite does not race appends. See docs/runbook.md for the SQL procedure.
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pyarrow as pa
from pyiceberg.expressions import (
    AlwaysTrue,
    And,
    BooleanExpression,
    EqualTo,
    GreaterThanOrEqual,
    Reference,
    literal,
)
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


def dedup_table(
    table: Table, row_filter: BooleanExpression | None = None
) -> tuple[int, int]:
    """Dedup an Iceberg table (optionally a time scope). Returns (before, after).

    A scoped rewrite (`row_filter`) avoids rewriting the whole table for a single
    rebalance duplicate; the filter prunes to the affected day partitions. Only
    rewrites when duplicates exist, so a clean scope produces no new commit.
    """
    scope = row_filter or AlwaysTrue()
    arrow = table.scan(row_filter=scope).to_arrow()
    before = arrow.num_rows
    deduped = dedup_arrow(arrow)
    after = deduped.num_rows
    if after < before:
        table.overwrite(deduped, overwrite_filter=scope)
    return before, after


def parse_key(raw: str) -> tuple[str, int]:
    """Parse a ``SYMBOL:TRADE_ID`` repair key."""
    symbol, sep, trade_id = raw.partition(":")
    if not sep or not symbol or not trade_id.isdigit():
        raise ValueError(f"expected SYMBOL:TRADE_ID, got {raw!r}")
    return symbol, int(trade_id)


def dedup_key(table: Table, symbol: str, trade_id: int) -> tuple[int, int]:
    """Repair one duplicate key by rewriting only the files that contain it.

    A day-scoped rewrite has to read/rewrite every file in the range (slow on an
    uncompacted table: 4k files). An equality filter on the key prunes manifests
    by symbol/trade-id stats and rewrites just the matching file(s).
    """
    key_filter = And(
        EqualTo(term=Reference("symbol"), value=literal(symbol)),
        EqualTo(term=Reference("trade_id"), value=literal(trade_id)),
    )
    arrow = table.scan(row_filter=key_filter).to_arrow()
    before = arrow.num_rows
    deduped = dedup_arrow(arrow)
    after = deduped.num_rows
    if after < before:
        table.overwrite(deduped, overwrite_filter=key_filter)
    return before, after


def run_dedup_key(
    settings: Settings, raw_key: str, table_name: str = BRONZE_TABLE
) -> DedupOutcome:
    """Repair a single ``SYMBOL:TRADE_ID`` duplicate. Returns the outcome; never raises."""
    try:
        symbol, trade_id = parse_key(raw_key)
        catalog = load_lake_catalog(settings)
        table = catalog.load_table(f"{settings.glue_database}.{table_name}")
        before, after = dedup_key(table, symbol, trade_id)
    except Exception as exc:  # noqa: BLE001 — surface as an outcome for the CLI
        logger.warning("dedup_key_failed", table=table_name, key=raw_key, error=str(exc))
        return DedupOutcome(table_name, 0, 0, ok=False, error=str(exc))
    logger.info(
        "dedup_key_ok",
        table=table_name,
        key=raw_key,
        rows_before=before,
        rows_after=after,
        removed=before - after,
    )
    return DedupOutcome(table_name, before, after, ok=True)


def run_dedup(
    settings: Settings, table_name: str = BRONZE_TABLE, days: int | None = None
) -> DedupOutcome:
    """Run the dedup rewrite once. Returns the outcome; never raises.

    ``days`` limits the rewrite to rows ingested in the last N days (the useful
    scope for a rebalance incident); ``None`` rewrites the whole table.
    """
    since = (
        datetime.now(UTC).replace(tzinfo=None) - timedelta(days=days)
        if days is not None
        else None
    )
    row_filter: BooleanExpression = (
        GreaterThanOrEqual(term=Reference("event_time"), value=literal(since))
        if since is not None
        else AlwaysTrue()
    )
    try:
        catalog = load_lake_catalog(settings)
        table = catalog.load_table(f"{settings.glue_database}.{table_name}")
        before, after = dedup_table(table, row_filter)
    except Exception as exc:  # noqa: BLE001 — surface as an outcome for the CLI
        logger.warning("dedup_failed", table=table_name, error=str(exc))
        return DedupOutcome(table_name, 0, 0, ok=False, error=str(exc))
    logger.info(
        "dedup_ok",
        table=table_name,
        scope_days=days,
        rows_before=before,
        rows_after=after,
        removed=before - after,
    )
    return DedupOutcome(table_name, before, after, ok=True)
