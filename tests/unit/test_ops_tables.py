"""Ops-table helpers: schemas, Arrow conversion, append/read-back (offline SQL catalog)."""

import json
from datetime import UTC, datetime
from pathlib import Path

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError

from pipeline.common.config import Settings
from pipeline.common.iceberg import (
    INTEGRITY_RESULTS_TABLE,
    LAYER_COUNTS_ARROW_SCHEMA,
    LAYER_COUNTS_TABLE,
    PIPELINE_METRICS_ARROW_SCHEMA,
    PIPELINE_METRICS_TABLE,
    QUALITY_RESULTS_ARROW_SCHEMA,
    QUALITY_RESULTS_TABLE,
    SYMBOL_FRESHNESS_ARROW_SCHEMA,
    SYMBOL_FRESHNESS_TABLE,
    append_arrow,
    ensure_ops_tables,
    layer_counts_to_arrow,
    load_lake_catalog,
    metrics_to_arrow,
    quality_result_to_arrow,
    symbol_freshness_to_arrow,
)
from pipeline.common.models import BatchReport, LayerCount, QualityResult, SymbolFreshness

NAMESPACE = "testdb"


def _settings() -> Settings:
    return Settings(catalog_type="sql", glue_database=NAMESPACE)


def _catalog(tmp_path: Path) -> tuple[Catalog, Settings]:
    settings = _settings()
    catalog = load_lake_catalog(settings, tmp_path)
    try:
        catalog.create_namespace(NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass
    return catalog, settings


def _quality_result() -> QualityResult:
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    return QualityResult(
        check_at=now,
        success=False,
        window_start=now,
        window_end=now,
        rows_scanned=10,
        failed_expectations=("expect_column_values_to_be_unique",),
        run_ms=12.5,
    )


def test_ensure_ops_tables_is_idempotent(tmp_path: Path) -> None:
    catalog, settings = _catalog(tmp_path)
    first = ensure_ops_tables(catalog, settings.glue_database)
    second = ensure_ops_tables(catalog, settings.glue_database)
    assert first.quality.name() == (NAMESPACE, QUALITY_RESULTS_TABLE)
    assert first.metrics.name() == (NAMESPACE, PIPELINE_METRICS_TABLE)
    assert first.integrity.name() == (NAMESPACE, INTEGRITY_RESULTS_TABLE)
    assert first.layer_counts.name() == (NAMESPACE, LAYER_COUNTS_TABLE)
    assert first.freshness.name() == (NAMESPACE, SYMBOL_FRESHNESS_TABLE)
    assert first.quality.metadata.location == second.quality.metadata.location
    assert first.metrics.metadata.location == second.metrics.metadata.location
    assert first.integrity.metadata.location == second.integrity.metadata.location
    assert first.layer_counts.metadata.location == second.layer_counts.metadata.location
    assert first.freshness.metadata.location == second.freshness.metadata.location


def test_quality_result_arrow_round_trip(tmp_path: Path) -> None:
    catalog, settings = _catalog(tmp_path)
    tables = ensure_ops_tables(catalog, settings.glue_database)
    quality = tables.quality
    result = _quality_result()
    arrow = quality_result_to_arrow(result)
    assert arrow.schema == QUALITY_RESULTS_ARROW_SCHEMA
    append_arrow(quality, arrow)
    row = quality.scan().to_arrow().to_pylist()[0]
    assert row["success"] is False
    assert row["rows_scanned"] == 10
    assert json.loads(row["failed_expectations"]) == ["expect_column_values_to_be_unique"]
    assert row["run_ms"] == 12.5


def test_metrics_arrow_handles_missing_max_event_time(tmp_path: Path) -> None:
    catalog, settings = _catalog(tmp_path)
    tables = ensure_ops_tables(catalog, settings.glue_database)
    metrics = tables.metrics
    report = BatchReport(rows=7, max_event_time=None, lag_messages=3, commit_ms=100.0)
    arrow = metrics_to_arrow(report, datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC))
    assert arrow.schema == PIPELINE_METRICS_ARROW_SCHEMA
    append_arrow(metrics, arrow)
    row = metrics.scan().to_arrow().to_pylist()[0]
    assert row["rows"] == 7
    assert row["max_event_time"] is None
    assert row["lag_messages"] == 3


def test_layer_counts_and_freshness_arrow_round_trip(tmp_path: Path) -> None:
    catalog, settings = _catalog(tmp_path)
    tables = ensure_ops_tables(catalog, settings.glue_database)
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)

    counts = layer_counts_to_arrow([LayerCount(collected_at=now, layer="bronze", row_count=100)])
    assert counts.schema == LAYER_COUNTS_ARROW_SCHEMA
    append_arrow(tables.layer_counts, counts)
    assert tables.layer_counts.scan().to_arrow().to_pylist()[0]["row_count"] == 100

    fresh = symbol_freshness_to_arrow(
        [
            SymbolFreshness(
                collected_at=now,
                symbol="ADAUSDT",
                max_event_time=now,
                freshness_seconds=0.0,
            )
        ]
    )
    assert fresh.schema == SYMBOL_FRESHNESS_ARROW_SCHEMA
    append_arrow(tables.freshness, fresh)
    assert tables.freshness.scan().to_arrow().to_pylist()[0]["symbol"] == "ADAUSDT"
