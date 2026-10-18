"""Whole-table bronze integrity check (ADR-016), offline SQL catalog."""

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.table import Table

from pipeline.common.config import Settings
from pipeline.common.iceberg import (
    INTEGRITY_RESULTS_ARROW_SCHEMA,
    INTEGRITY_RESULTS_TABLE,
    ensure_bronze_table,
    ensure_ops_tables,
    events_to_arrow,
    integrity_result_to_arrow,
    load_lake_catalog,
)
from pipeline.common.models import IntegrityResult, TradeEvent
from pipeline.quality.integrity import check_integrity, run_integrity

NAMESPACE = "testdb"


def _settings() -> Settings:
    return Settings(catalog_type="sql", glue_database=NAMESPACE)


def _event(trade_id: int) -> TradeEvent:
    when = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    return TradeEvent(
        symbol="ADAUSDT",
        trade_id=trade_id,
        price=Decimal("0.22"),
        quantity=Decimal("10"),
        event_time=when,
        ingest_time=when,
        is_buyer_maker=False,
    )


def _catalog(tmp_path: Path) -> tuple[Catalog, Settings]:
    settings = _settings()
    catalog = load_lake_catalog(settings, tmp_path)
    try:
        catalog.create_namespace(NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass
    return catalog, settings


def _seeded_bronze(tmp_path: Path, rows: list[TradeEvent]) -> Table:
    catalog, settings = _catalog(tmp_path)
    table = ensure_bronze_table(catalog, NAMESPACE)
    table.append(events_to_arrow(rows))
    return catalog.load_table(f"{NAMESPACE}.bronze_trades")


def test_check_integrity_clean_table(tmp_path: Path) -> None:
    table = _seeded_bronze(tmp_path, [_event(1), _event(2), _event(3)])
    result = check_integrity(table)
    assert (result.total_rows, result.distinct_keys, result.duplicates) == (3, 3, 0)
    assert result.ok is True


def test_check_integrity_counts_duplicates(tmp_path: Path) -> None:
    table = _seeded_bronze(tmp_path, [_event(1), _event(1), _event(2)])
    result = check_integrity(table)
    assert result.total_rows == 3
    assert result.distinct_keys == 2
    assert result.duplicates == 1
    assert result.ok is False


def test_run_integrity_appends_ops_row(tmp_path: Path) -> None:
    _seeded_bronze(tmp_path, [_event(1), _event(2)])
    result = run_integrity(_settings(), warehouse_dir=tmp_path)
    assert result.ok is True
    catalog, _ = _catalog(tmp_path)
    ops = ensure_ops_tables(catalog, NAMESPACE)
    rows = ops.integrity.scan().to_arrow().to_pylist()
    assert rows[-1]["table_name"] == "bronze_trades"
    assert rows[-1]["duplicates"] == 0


def test_integrity_arrow_schema_and_values() -> None:
    result = IntegrityResult(
        checked_at=datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC),
        table_name="bronze_trades",
        total_rows=10,
        distinct_keys=9,
        duplicates=1,
        run_ms=5.0,
    )
    arrow = integrity_result_to_arrow(result)
    assert arrow.schema == INTEGRITY_RESULTS_ARROW_SCHEMA
    row = arrow.to_pylist()[0]
    assert row["duplicates"] == 1 and row["table_name"] == "bronze_trades"
    assert INTEGRITY_RESULTS_TABLE == "integrity_results"
