"""Maintenance: statement order/content and per-statement failure isolation."""

from pipeline.common.config import Settings
from pipeline.scheduler.maintenance import build_statements, run_maintenance


def _settings() -> Settings:
    return Settings(catalog_type="sql", glue_database="testdb")


def test_build_statements_optimize_then_vacuum_per_table() -> None:
    assert build_statements("tickstream") == [
        ("bronze_trades", "OPTIMIZE tickstream.bronze_trades REWRITE DATA USING BIN_PACK"),
        ("bronze_trades", "VACUUM tickstream.bronze_trades"),
        ("silver_trades", "OPTIMIZE tickstream.silver_trades REWRITE DATA USING BIN_PACK"),
        ("silver_trades", "VACUUM tickstream.silver_trades"),
        ("gold_candles_1m", "OPTIMIZE tickstream.gold_candles_1m REWRITE DATA USING BIN_PACK"),
        ("gold_candles_1m", "VACUUM tickstream.gold_candles_1m"),
    ]


def test_run_maintenance_all_ok() -> None:
    executed: list[str] = []
    outcomes = run_maintenance(_settings(), executor=executed.append)
    assert len(outcomes) == 6
    assert all(outcome.ok and outcome.error is None for outcome in outcomes)
    assert executed == [sql for _, sql in build_statements("testdb")]


def test_run_maintenance_isolates_failures() -> None:
    def flaky(sql: str) -> None:
        if sql.startswith("VACUUM"):
            raise RuntimeError("boom")

    outcomes = run_maintenance(_settings(), executor=flaky)
    assert [outcome.ok for outcome in outcomes] == [True, False] * 3
    assert all("boom" in (outcome.error or "") for outcome in outcomes if not outcome.ok)
