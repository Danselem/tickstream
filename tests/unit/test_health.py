"""Pipeline health metrics (ADR-021): layer counts + per-symbol freshness, offline."""

from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, TimestampType

from pipeline.common.config import Settings
from pipeline.common.iceberg import (
    LAYER_COUNTS_TABLE,
    SYMBOL_FRESHNESS_TABLE,
    ensure_ops_tables,
    load_lake_catalog,
)
from pipeline.quality.health import compute_health, run_pipeline_health

NAMESPACE = "testdb"


def _settings() -> Settings:
    return Settings(catalog_type="sql", glue_database=NAMESPACE)


def _catalog(tmp_path: Path) -> Catalog:
    settings = _settings()
    catalog = load_lake_catalog(settings, tmp_path)
    try:
        catalog.create_namespace(NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass
    return catalog


def _layer_schema() -> Schema:
    return Schema(
        NestedField(1, "symbol", StringType(), required=True),
        NestedField(2, "candle_start", TimestampType(), required=True),
    )


_LAYER_ARROW = pa.schema(
    [
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("candle_start", pa.timestamp("us"), nullable=False),
    ]
)


def _seed(catalog: Catalog, name: str, rows: list[tuple[str, datetime]]) -> None:
    table = catalog.create_table_if_not_exists(f"{NAMESPACE}.{name}", schema=_layer_schema())
    if rows:
        table.append(
            pa.table(
                {
                    "symbol": [r[0] for r in rows],
                    "candle_start": [r[1].replace(tzinfo=None) for r in rows],
                },
                schema=_LAYER_ARROW,
            )
        )


def test_compute_health_counts_and_freshness(tmp_path: Path) -> None:
    catalog = _catalog(tmp_path)
    now = datetime(2026, 1, 1, 12, 10, 0, tzinfo=UTC)
    _seed(catalog, "bronze_trades", [("ADAUSDT", now), ("ADAUSDT", now)])
    _seed(catalog, "silver_trades", [("ADAUSDT", now)])
    _seed(
        catalog,
        "gold_candles_1m",
        [
            ("ADAUSDT", datetime(2026, 1, 1, 12, 5, tzinfo=UTC)),
            ("BTCUSDT", datetime(2026, 1, 1, 12, 0, tzinfo=UTC)),
        ],
    )
    layers = [
        (name, catalog.load_table(f"{NAMESPACE}.{table}"))
        for name, table in (
            ("bronze", "bronze_trades"),
            ("silver", "silver_trades"),
            ("gold", "gold_candles_1m"),
        )
    ]
    gold = catalog.load_table(f"{NAMESPACE}.gold_candles_1m")
    counts, freshness = compute_health(layers=layers, gold=gold, now=now)

    assert [c.layer for c in counts] == ["bronze", "silver", "gold"]
    assert [c.row_count for c in counts] == [2, 1, 2]
    by_symbol = {f.symbol: f.freshness_seconds for f in freshness}
    assert by_symbol["ADAUSDT"] == 300.0
    assert by_symbol["BTCUSDT"] == 600.0


def test_run_pipeline_health_appends_ops_rows(tmp_path: Path) -> None:
    catalog = _catalog(tmp_path)
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    _seed(catalog, "bronze_trades", [("ADAUSDT", now)])
    _seed(catalog, "silver_trades", [("ADAUSDT", now)])
    _seed(catalog, "gold_candles_1m", [("ADAUSDT", now)])

    counts, freshness = run_pipeline_health(_settings(), warehouse_dir=tmp_path)
    assert len(counts) == 3 and len(freshness) == 1

    ops = ensure_ops_tables(catalog, NAMESPACE)
    count_rows = ops.layer_counts.scan().to_arrow().to_pylist()
    fresh_rows = ops.freshness.scan().to_arrow().to_pylist()
    assert {r["layer"] for r in count_rows} == {"bronze", "silver", "gold"}
    assert fresh_rows[0]["symbol"] == "ADAUSDT"
    assert LAYER_COUNTS_TABLE == "layer_counts"
    assert SYMBOL_FRESHNESS_TABLE == "symbol_freshness"
