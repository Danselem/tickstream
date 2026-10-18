"""Scheduler loop: GX check -> dbt build (only on pass) -> periodic maintenance.

`python -m pipeline.scheduler --once --quality-only` backs `make quality`.
A failed GX run returns 1 and dbt is never invoked (Brief §6).
"""

import argparse
import signal
import time
from collections.abc import Callable, Sequence

from pipeline.common.config import Settings, get_settings
from pipeline.common.logging import configure_logging, get_logger
from pipeline.quality import run_quality
from pipeline.scheduler.dbt_runner import run_dbt_build
from pipeline.scheduler.maintenance import MAINTENANCE_TABLES, run_maintenance
from pipeline.scheduler.repair import run_dedup

logger = get_logger("scheduler")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """CLI: --once (single cycle), --quality-only (skip dbt), --maintenance,
    --maintenance-only (OPTIMIZE/VACUUM only, bypassing the quality gate)."""
    parser = argparse.ArgumentParser(
        description="tickstream scheduler: quality -> dbt -> maintenance"
    )
    parser.add_argument("--once", action="store_true", help="run a single cycle and exit")
    parser.add_argument(
        "--quality-only", action="store_true", help="run GX only, skip dbt and maintenance"
    )
    parser.add_argument(
        "--maintenance",
        action="store_true",
        help="also run OPTIMIZE + VACUUM after a passing dbt build",
    )
    parser.add_argument(
        "--maintenance-only",
        action="store_true",
        help="run OPTIMIZE + VACUUM and exit, bypassing quality and dbt",
    )
    parser.add_argument(
        "--tables",
        default=",".join(MAINTENANCE_TABLES),
        help="comma-separated tables for --maintenance-only (default: all medallion tables)",
    )
    parser.add_argument(
        "--dedup-bronze",
        action="store_true",
        help="one-time bronze dedup rewrite via Athena (stop consumers first)",
    )
    return parser.parse_args(argv)


def run_maintenance_only(settings: Settings, tables: Sequence[str]) -> int:
    """Compact/clean the given tables without the quality gate. 0 on all-ok, 1 otherwise."""
    outcomes = run_maintenance(settings, tables=tables)
    return 0 if all(outcome.ok for outcome in outcomes) else 1


def run_once(
    settings: Settings, *, quality_only: bool = False, with_maintenance: bool = False
) -> int:
    """One cycle. Returns 0 on full success, 1 on GX failure, dbt's code otherwise."""
    quality = run_quality(settings)
    if not quality.success:
        logger.error(
            "quality_failed_dbts_skipped",
            failed_expectations=list(quality.failed_expectations),
            rows_scanned=quality.rows_scanned,
        )
        return 1
    if not quality_only:
        dbt_returncode = run_dbt_build()
        if dbt_returncode != 0:
            logger.error("dbt_failed", returncode=dbt_returncode)
            return dbt_returncode
    if with_maintenance:
        run_maintenance(settings)
    return 0


def _sleep_interruptible(total_s: float, alive: Callable[[], bool]) -> None:
    """Sleep in 1 s slices so SIGTERM stops the loop promptly."""
    deadline = time.monotonic() + total_s
    while alive() and time.monotonic() < deadline:
        time.sleep(min(1.0, deadline - time.monotonic()))


def main(argv: Sequence[str] | None = None) -> int:
    """Loop every quality_interval_s; maintenance every Nth cycle. `--once` runs one cycle."""
    args = parse_args(argv)
    settings = get_settings()
    configure_logging(settings.log_level)
    if args.dedup_bronze:
        return 0 if run_dedup(settings).ok else 1
    if args.maintenance_only:
        tables = tuple(t.strip() for t in args.tables.split(",") if t.strip())
        return run_maintenance_only(settings, tables)
    if args.once:
        return run_once(
            settings,
            quality_only=args.quality_only,
            with_maintenance=args.maintenance,
        )

    running = True

    def _stop(_signum: int, _frame: object) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    cycle = 0
    logger.info("scheduler_started", interval_s=settings.quality_interval_s)
    while running:
        cycle += 1
        returncode = run_once(
            settings,
            with_maintenance=(cycle % settings.maintenance_every_n_runs == 0),
        )
        logger.info("scheduler_cycle_done", cycle=cycle, returncode=returncode)
        _sleep_interruptible(float(settings.quality_interval_s), lambda: running)
    logger.info("scheduler_stopped", cycles_completed=cycle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
