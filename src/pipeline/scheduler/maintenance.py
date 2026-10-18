"""Athena Iceberg maintenance: OPTIMIZE (compaction) + VACUUM (snapshot cleanup).

Runs every `maintenance_every_n_runs` scheduler cycles (≈12 h) plus on demand via
`--maintenance`. Failures are logged per statement and never crash the loop.
Note: Athena deletes at most 20,000 objects per VACUUM execution.
"""

from collections.abc import Callable, Sequence
from dataclasses import dataclass

from pipeline.common.config import Settings
from pipeline.common.logging import get_logger

logger = get_logger("scheduler.maintenance")

# Provisioned by infra/athena.tf; engine AUTO tracks v3 (MERGE/OPTIMIZE/VACUUM).
WORKGROUP = "tickstream-pipeline"

MAINTENANCE_TABLES = (
    "bronze_trades",
    "silver_trades",
    "gold_candles_1m",
    "gold_market_summary_daily",
    "gold_market_summary_hourly",
)

StatementExecutor = Callable[[str], None]


@dataclass(frozen=True)
class MaintenanceOutcome:
    """Per-statement result: OPTIMIZE and VACUUM for each medallion table."""

    table: str
    statement: str
    ok: bool
    error: str | None = None


def build_statements(
    database: str, tables: Sequence[str] = MAINTENANCE_TABLES
) -> list[tuple[str, str]]:
    """(table, sql) pairs: OPTIMIZE then VACUUM per table, in dependency order."""
    statements: list[tuple[str, str]] = []
    for table in tables:
        qualified = f"{database}.{table}"
        statements.append((table, f"OPTIMIZE {qualified} REWRITE DATA USING BIN_PACK"))
        statements.append((table, f"VACUUM {qualified}"))
    return statements


def _athena_executor(settings: Settings) -> StatementExecutor:
    """Blocking pyathena executor; credentials come from the standard AWS chain."""
    from pyathena import connect  # local import: only needed when maintenance runs

    cursor = connect(
        region_name=settings.aws_region,
        s3_staging_dir=f"s3://{settings.s3_bucket}/athena-results/",
        schema_name=settings.glue_database,
        work_group=WORKGROUP,
    ).cursor()

    def execute(sql: str) -> None:
        cursor.execute(sql)

    return execute


def run_maintenance(
    settings: Settings,
    executor: StatementExecutor | None = None,
    tables: Sequence[str] = MAINTENANCE_TABLES,
) -> list[MaintenanceOutcome]:
    """Execute OPTIMIZE + VACUUM per table. Always returns per-statement outcomes."""
    execute = executor if executor is not None else _athena_executor(settings)
    outcomes: list[MaintenanceOutcome] = []
    for table, sql in build_statements(settings.glue_database, tables):
        try:
            execute(sql)
        except Exception as exc:  # noqa: BLE001 — one table must not block the rest
            outcomes.append(MaintenanceOutcome(table, sql, False, str(exc)))
            logger.warning("maintenance_failed", table=table, sql=sql, error=str(exc))
        else:
            outcomes.append(MaintenanceOutcome(table, sql, True))
            logger.info("maintenance_ok", table=table, verb=sql.split()[0])
    return outcomes
