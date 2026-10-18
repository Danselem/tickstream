"""Pipeline health metrics (ADR-021): per-layer row counts and per-symbol freshness.

Runs on the scheduler's maintenance cadence, after the integrity check, and
appends to the `layer_counts` and `symbol_freshness` ops tables. Row counts come
from Iceberg snapshot metadata (O(1) per layer); freshness scans only gold's
`(symbol, candle_start)` columns so it stays cheap. Together they answer "is the
data arriving, and is anything being lost between layers?".
"""

import time
from datetime import UTC, datetime
from pathlib import Path

from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table import Table

from pipeline.common.config import Settings
from pipeline.common.iceberg import (
    append_arrow,
    ensure_ops_tables,
    layer_counts_to_arrow,
    load_lake_catalog,
    symbol_freshness_to_arrow,
    table_row_count,
)
from pipeline.common.logging import get_logger
from pipeline.common.models import LayerCount, SymbolFreshness

logger = get_logger("quality.health")

# Medallion tables in order (bronze -> silver -> gold).
LAYER_TABLES: tuple[tuple[str, str], ...] = (
    ("bronze", "bronze_trades"),
    ("silver", "silver_trades"),
    ("gold", "gold_candles_1m"),
)
GOLD_TABLE = "gold_candles_1m"


def compute_health(
    *,
    layers: list[tuple[str, Table]],
    gold: Table,
    now: datetime,
) -> tuple[list[LayerCount], list[SymbolFreshness]]:
    """Pure computation: layer row counts + per-symbol freshness as of ``now``."""
    counts = [
        LayerCount(collected_at=now, layer=name, row_count=table_row_count(table))
        for name, table in layers
    ]
    frame = gold.scan(selected_fields=("symbol", "candle_start")).to_arrow().to_pandas()
    freshness: list[SymbolFreshness] = []
    if not frame.empty:
        latest = frame.groupby("symbol")["candle_start"].max()
        for symbol, value in latest.items():
            event_time = value.to_pydatetime()
            if event_time.tzinfo is None:
                event_time = event_time.replace(tzinfo=UTC)
            freshness.append(
                SymbolFreshness(
                    collected_at=now,
                    symbol=str(symbol),
                    max_event_time=event_time,
                    freshness_seconds=(now - event_time).total_seconds(),
                )
            )
    return counts, freshness


def run_pipeline_health(
    settings: Settings, warehouse_dir: Path | None = None
) -> tuple[list[LayerCount], list[SymbolFreshness]]:
    """Collect health metrics and append them to the ops tables."""
    started = time.monotonic()
    catalog = load_lake_catalog(settings, warehouse_dir)
    ops = ensure_ops_tables(catalog, settings.glue_database)

    layers: list[tuple[str, Table]] = []
    for name, table_name in LAYER_TABLES:
        try:
            layers.append(
                (name, catalog.load_table(f"{settings.glue_database}.{table_name}"))
            )
        except NoSuchTableError:
            logger.warning("health_layer_missing", layer=name, table=table_name)

    gold = next((table for name, table in layers if name == "gold"), None)
    if gold is None:
        logger.warning("health_skipped_no_gold")
        return [], []

    counts, freshness = compute_health(layers=layers, gold=gold, now=datetime.now(UTC))
    append_arrow(ops.layer_counts, layer_counts_to_arrow(counts))
    append_arrow(ops.freshness, symbol_freshness_to_arrow(freshness))
    logger.info(
        "health_collected",
        layers={count.layer: count.row_count for count in counts},
        symbols=len(freshness),
        run_ms=(time.monotonic() - started) * 1000,
    )
    return counts, freshness
