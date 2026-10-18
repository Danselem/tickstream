"""Scheduler gating: a failed GX run must block dbt; maintenance follows passes."""

from datetime import UTC, datetime

import pytest

from pipeline.common.config import Settings
from pipeline.common.models import QualityResult
from pipeline.scheduler import run as run_module
from pipeline.scheduler.run import parse_args, run_once


def _settings() -> Settings:
    return Settings()


def _ok_result() -> QualityResult:
    now = datetime.now(UTC)
    return QualityResult(
        check_at=now, success=True, window_start=now, window_end=now, rows_scanned=5
    )


def _failed_result() -> QualityResult:
    now = datetime.now(UTC)
    return QualityResult(
        check_at=now,
        success=False,
        window_start=now,
        window_end=now,
        rows_scanned=5,
        failed_expectations=("expect_column_values_to_be_unique",),
    )


def test_quality_failure_blocks_dbt(monkeypatch: pytest.MonkeyPatch) -> None:
    dbt_calls: list[str] = []

    def fake_quality(settings: Settings) -> QualityResult:
        return _failed_result()

    def fake_dbt() -> int:
        dbt_calls.append("dbt")
        return 0

    monkeypatch.setattr(run_module, "run_quality", fake_quality)
    monkeypatch.setattr(run_module, "run_dbt_build", fake_dbt)

    assert run_once(_settings(), with_maintenance=True) == 1
    assert dbt_calls == []


def test_quality_only_skips_dbt_on_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    dbt_calls: list[str] = []

    def fake_quality(settings: Settings) -> QualityResult:
        return _ok_result()

    def fake_dbt() -> int:
        dbt_calls.append("dbt")
        return 0

    monkeypatch.setattr(run_module, "run_quality", fake_quality)
    monkeypatch.setattr(run_module, "run_dbt_build", fake_dbt)

    assert run_once(_settings(), quality_only=True) == 0
    assert dbt_calls == []


def test_quality_only_with_maintenance_skips_dbt_but_compacts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def fake_quality(settings: Settings) -> QualityResult:
        return _ok_result()

    def fake_dbt() -> int:
        calls.append("dbt")
        return 0

    def fake_maintenance(settings: Settings) -> list[object]:
        calls.append("maintenance")
        return []

    monkeypatch.setattr(run_module, "run_quality", fake_quality)
    monkeypatch.setattr(run_module, "run_dbt_build", fake_dbt)
    monkeypatch.setattr(run_module, "run_maintenance", fake_maintenance)

    assert run_once(_settings(), quality_only=True, with_maintenance=True) == 0
    assert calls == ["maintenance"]


def test_pass_runs_dbt_then_maintenance(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def fake_quality(settings: Settings) -> QualityResult:
        return _ok_result()

    def fake_dbt() -> int:
        calls.append("dbt")
        return 0

    def fake_maintenance(settings: Settings) -> list[object]:
        calls.append("maintenance")
        return []

    monkeypatch.setattr(run_module, "run_quality", fake_quality)
    monkeypatch.setattr(run_module, "run_dbt_build", fake_dbt)
    monkeypatch.setattr(run_module, "run_maintenance", fake_maintenance)

    assert run_once(_settings(), with_maintenance=True) == 0
    assert calls == ["dbt", "maintenance"]


def test_dbt_failure_propagates_without_maintenance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    maintenance_calls: list[str] = []

    def fake_quality(settings: Settings) -> QualityResult:
        return _ok_result()

    def fake_maintenance(settings: Settings) -> list[object]:
        maintenance_calls.append("maintenance")
        return []

    monkeypatch.setattr(run_module, "run_quality", fake_quality)
    monkeypatch.setattr(run_module, "run_dbt_build", lambda: 2)
    monkeypatch.setattr(run_module, "run_maintenance", fake_maintenance)

    assert run_once(_settings(), with_maintenance=True) == 2
    assert maintenance_calls == []


def test_parse_args_flags() -> None:
    args = parse_args(["--once", "--quality-only"])
    assert args.once is True and args.quality_only is True and args.maintenance is False
    args = parse_args(["--once", "--quality-only", "--maintenance"])
    assert args.maintenance is True
    args = parse_args([])
    assert args.once is False


def test_parse_args_maintenance_only_and_tables() -> None:
    args = parse_args(["--once", "--maintenance-only", "--tables", "bronze_trades"])
    assert args.maintenance_only is True
    assert args.tables == "bronze_trades"
    assert parse_args([]).maintenance_only is False
    # default tables is all medallion tables
    assert set(parse_args([]).tables.split(",")) == {
        "bronze_trades",
        "silver_trades",
        "gold_candles_1m",
    }


def test_maintenance_only_bypasses_quality_and_dbt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def fake_quality(settings: Settings) -> QualityResult:
        calls.append("quality")
        return _ok_result()

    def fake_dbt() -> int:
        calls.append("dbt")
        return 0

    def fake_maintenance(settings: Settings, tables: object = ()) -> list[object]:
        calls.append(f"maintenance:{tables}")
        return []

    monkeypatch.setattr(run_module, "run_quality", fake_quality)
    monkeypatch.setattr(run_module, "run_dbt_build", fake_dbt)
    monkeypatch.setattr(run_module, "run_maintenance", fake_maintenance)

    assert run_module.run_maintenance_only(_settings(), ("bronze_trades",)) == 0
    assert calls == ["maintenance:('bronze_trades',)"]


def test_maintenance_only_returns_one_on_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pipeline.scheduler.maintenance import MaintenanceOutcome

    def fake_maintenance(settings: Settings, tables: object = ()) -> list[object]:
        return [MaintenanceOutcome("bronze_trades", "VACUUM", ok=False, error="boom")]

    monkeypatch.setattr(run_module, "run_maintenance", fake_maintenance)
    assert run_module.run_maintenance_only(_settings(), ("bronze_trades",)) == 1

