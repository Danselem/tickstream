"""CLI: backfill Binance 1m klines into bronze. ``python -m pipeline.backfill``."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta

from pipeline.backfill.klines import build_backfill, write_backfill
from pipeline.common.config import get_settings
from pipeline.common.logging import configure_logging, get_logger

logger = get_logger("backfill")


def parse_date(value: str) -> date:
    """Parse an ISO date (UTC)."""
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=UTC).date()


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--start",
        type=parse_date,
        default=None,
        help="first day to backfill (default: first day of the month 60 days ago)",
    )
    parser.add_argument(
        "--end",
        type=parse_date,
        default=None,
        help="last day to backfill (default: yesterday UTC)",
    )
    parser.add_argument(
        "--symbols",
        default=None,
        help="comma-separated symbols (default: TICKSTREAM_SYMBOLS)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="replace the existing bronze_klines table instead of skipping",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    settings = get_settings()
    configure_logging(settings.log_level)
    end = args.end or (datetime.now(UTC).date() - timedelta(days=1))
    start = args.start or (end - timedelta(days=60)).replace(day=1)
    if start > end:
        logger.error("backfill_bad_range", start=str(start), end=str(end))
        return 2
    symbols = (
        [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
        if args.symbols
        else list(settings.symbols)
    )
    logger.info(
        "backfill_started", symbols=symbols, start=str(start), end=str(end), force=args.force
    )
    try:
        arrow = build_backfill(symbols, start, end)
        written = write_backfill(settings, arrow, force=args.force)
    except Exception as exc:  # noqa: BLE001 — CLI exit code, details in the log
        logger.error("backfill_failed", error=str(exc))
        return 1
    logger.info("backfill_done", rows=arrow.num_rows, written=written)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
