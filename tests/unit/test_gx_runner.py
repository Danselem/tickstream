"""GX runner tests against an offline SQL catalog (no AWS).

Covers the Brief §6 checks: schema/columns, non-null keys, price/quantity > 0,
trade-id uniqueness, freshness, plus the scan-window rule (last success minus
5-min overlap) and the empty-window failure path.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.table import Table

from pipeline.common.config import Settings
from pipeline.common.iceberg import (
    BRONZE_ARROW_SCHEMA,
    append_arrow,
    ensure_bronze_table,
    ensure_ops_tables,
    load_lake_catalog,
    quality_result_to_arrow,
    upsert_events,
)
from pipeline.common.models import QualityResult, TradeEvent
from pipeline.quality import gx_runner
from pipeline.quality.gx_runner import (
    build_bronze_suite,
    resolve_window,
    run_quality,
)

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


def _isolate_artifacts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep JSON reports / suite snapshots out of the real quality/ dir."""
    monkeypatch.setattr(gx_runner, "REPORTS_DIR", tmp_path / "reports")
    monkeypatch.setattr(gx_runner, "SUITE_PATH", tmp_path / "suites" / "bronze.json")


def _event(
    trade_id: int,
    event_time: datetime,
    price: Decimal = Decimal("1.50000"),
) -> TradeEvent:
    return TradeEvent(
        symbol="XRPUSDT",
        trade_id=trade_id,
        price=price,
        quantity=Decimal("10.0"),
        event_time=event_time,
        ingest_time=event_time,
        is_buyer_maker=False,
    )


def _write_bronze(catalog: Catalog, settings: Settings, events: list[TradeEvent]) -> Table:
    table = ensure_bronze_table(catalog, settings.glue_database)
    if events:
        upsert_events(table, events)
    return table


def test_quality_passes_on_clean_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _isolate_artifacts(tmp_path, monkeypatch)
    catalog, settings = _catalog(tmp_path)
    now = datetime.now(UTC)
    _write_bronze(catalog, settings, [_event(i, now - timedelta(seconds=i * 10)) for i in range(5)])

    result = run_quality(settings, tmp_path)

    assert result.success is True
    assert result.rows_scanned == 5
    assert result.failed_expectations == ()
    assert (tmp_path / "reports").glob("*.json")
    assert (tmp_path / "suites" / "bronze.json").exists()
    quality_table = ensure_ops_tables(catalog, settings.glue_database).quality
    rows = quality_table.scan().to_arrow().to_pylist()
    assert len(rows) == 1 and rows[0]["success"] is True


def test_quality_fails_on_non_positive_price(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _isolate_artifacts(tmp_path, monkeypatch)
    catalog, settings = _catalog(tmp_path)
    now = datetime.now(UTC)
    table = _write_bronze(catalog, settings, [_event(1, now)])
    bad = pa.Table.from_pylist(
        [
            {
                "symbol": "XRPUSDT",
                "trade_id": 2,
                "price": Decimal("-0.5"),
                "quantity": Decimal("10.0"),
                "event_time": now.replace(tzinfo=None),
                "ingest_time": now.replace(tzinfo=None),
                "is_buyer_maker": False,
            }
        ],
        schema=BRONZE_ARROW_SCHEMA,
    )
    table.append(bad)  # bypasses pydantic: negative prices never come from TradeEvent

    result = run_quality(settings, tmp_path)

    assert result.success is False
    assert "expect_column_values_to_be_between" in result.failed_expectations


def test_quality_fails_on_duplicate_trade_ids(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _isolate_artifacts(tmp_path, monkeypatch)
    catalog, settings = _catalog(tmp_path)
    now = datetime.now(UTC)
    table = _write_bronze(catalog, settings, [_event(1, now)])
    table.append(table.scan().to_arrow())  # raw duplicate: upsert would absorb this

    result = run_quality(settings, tmp_path)

    assert result.success is False
    assert "expect_column_values_to_be_unique" in result.failed_expectations


def test_quality_fails_on_stale_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _isolate_artifacts(tmp_path, monkeypatch)
    catalog, settings = _catalog(tmp_path)
    stale = datetime.now(UTC) - timedelta(hours=1)
    _write_bronze(catalog, settings, [_event(1, stale)])

    result = run_quality(settings, tmp_path)

    assert result.success is False
    assert "expect_column_max_to_be_between" in result.failed_expectations


def test_empty_bronze_fails_with_empty_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _isolate_artifacts(tmp_path, monkeypatch)
    catalog, settings = _catalog(tmp_path)
    _write_bronze(catalog, settings, [])

    result = run_quality(settings, tmp_path)

    assert result.success is False
    assert result.rows_scanned == 0
    assert result.failed_expectations == ("empty_window",)


def test_window_starts_at_last_success_minus_overlap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _isolate_artifacts(tmp_path, monkeypatch)
    catalog, settings = _catalog(tmp_path)
    now = datetime.now(UTC)
    quality_table = ensure_ops_tables(catalog, settings.glue_database).quality
    prior = QualityResult(
        check_at=now - timedelta(minutes=10),
        success=True,
        window_start=now - timedelta(minutes=20),
        window_end=now - timedelta(minutes=10),
        rows_scanned=3,
        run_ms=1.0,
    )
    append_arrow(quality_table, quality_result_to_arrow(prior))

    # Outside [last_success - 5 min, now]: must be excluded from the scan.
    _write_bronze(
        catalog,
        settings,
        [
            _event(1, now - timedelta(minutes=20)),
            _event(2, now - timedelta(seconds=30)),
        ],
    )

    result = run_quality(settings, tmp_path)

    assert result.success is True
    assert result.rows_scanned == 1


def test_suite_contains_brief_section_checks() -> None:
    now = datetime.now(UTC)
    suite = build_bronze_suite(freshness_cutoff=now - timedelta(seconds=300), now=now)
    names = [type(expectation).__name__ for expectation in suite.expectations]
    assert "ExpectTableColumnsToMatchSet" in names
    assert names.count("ExpectColumnValuesToNotBeNull") == 7
    assert "ExpectColumnValuesToBeUnique" in names
    assert names.count("ExpectColumnValuesToBeBetween") == 2
    assert "ExpectColumnMaxToBeBetween" in names
    assert "ExpectTableRowCountToBeBetween" in names


def test_resolve_window_first_run_looks_back_one_day(tmp_path: Path) -> None:
    catalog, settings = _catalog(tmp_path)
    quality_table = ensure_ops_tables(catalog, settings.glue_database).quality
    now = datetime.now(UTC)
    start, end = resolve_window(quality_table, now)
    assert end == now
    assert start == now - timedelta(days=1)
