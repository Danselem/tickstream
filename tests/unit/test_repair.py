"""Bronze dedup repair: Arrow dedup + in-place Iceberg rewrite (offline)."""

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.table import Table

from pipeline.common.config import Settings
from pipeline.common.iceberg import (
    BRONZE_TABLE,
    ensure_bronze_table,
    events_to_arrow,
    load_lake_catalog,
)
from pipeline.common.models import TradeEvent
from pipeline.scheduler import repair as repair_module
from pipeline.scheduler import run as run_module
from pipeline.scheduler.repair import (
    DedupOutcome,
    dedup_arrow,
    dedup_table,
    run_dedup,
)

NAMESPACE = "testdb"


def _settings() -> Settings:
    return Settings(catalog_type="sql", glue_database=NAMESPACE)


def _event(trade_id: int, second: int) -> TradeEvent:
    when = datetime(2026, 1, 1, 12, 0, second, tzinfo=UTC)
    return TradeEvent(
        symbol="ADAUSDT",
        trade_id=trade_id,
        price=Decimal("0.2203"),
        quantity=Decimal("10"),
        event_time=when,
        ingest_time=when,
        is_buyer_maker=False,
    )


def _catalog_with_dupes(tmp_path: Path) -> Table:
    settings = _settings()
    catalog = load_lake_catalog(settings, tmp_path)
    try:
        catalog.create_namespace(NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass
    table = ensure_bronze_table(catalog, NAMESPACE)
    # Bypass the writer's anti-join to emulate the duplication bug: append the
    # same key twice with identical rows (as a rebalance replay produced).
    table.append(events_to_arrow([_event(1, 0), _event(1, 0), _event(2, 1)]))
    return catalog.load_table(f"{NAMESPACE}.{BRONZE_TABLE}")


def test_dedup_arrow_keeps_one_row_per_key() -> None:
    arrow = events_to_arrow([_event(1, 0), _event(1, 0), _event(2, 1), _event(2, 1)])
    deduped = dedup_arrow(arrow)
    assert deduped.num_rows == 2
    assert sorted(deduped["trade_id"].to_pylist()) == [1, 2]


def test_dedup_arrow_keeps_earliest_ingest() -> None:
    early = _event(1, 0)
    late_ingest = datetime(2026, 1, 1, 12, 0, 9, tzinfo=UTC)
    late = early.model_copy(update={"price": Decimal("0.9999"), "ingest_time": late_ingest})
    deduped = dedup_arrow(events_to_arrow([late, early]))  # same key, two ingests
    assert deduped.num_rows == 1
    assert Decimal(deduped["price"].to_pylist()[0]) == Decimal("0.2203")  # earliest


def test_dedup_table_rewrites_and_removes_dupes(tmp_path: Path) -> None:
    table = _catalog_with_dupes(tmp_path)
    assert table.scan().to_arrow().num_rows == 3
    before, after = dedup_table(table)
    assert (before, after) == (3, 2)
    assert table.scan().to_arrow().num_rows == 2


def test_dedup_table_clean_table_is_noop(tmp_path: Path) -> None:
    table = _catalog_with_dupes(tmp_path)
    table.overwrite(dedup_arrow(table.scan().to_arrow()))  # make it clean
    before, after = dedup_table(table)
    assert before == after == 2


def test_run_dedup_reports_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError("glue down")

    monkeypatch.setattr(repair_module, "load_lake_catalog", boom)
    outcome = run_dedup(_settings())
    assert outcome.ok is False and outcome.error == "glue down"


def test_cli_dedup_bronze_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    called: list[str] = []

    def fake_run_dedup(settings: Settings) -> DedupOutcome:
        called.append("dedup")
        return DedupOutcome(BRONZE_TABLE, 3, 2, ok=True)

    monkeypatch.setattr(run_module, "run_dedup", fake_run_dedup)
    monkeypatch.setattr(run_module, "get_settings", _settings)
    monkeypatch.setattr(run_module, "configure_logging", lambda *_: None)
    assert run_module.main(["--dedup-bronze"]) == 0
    assert called == ["dedup"]
