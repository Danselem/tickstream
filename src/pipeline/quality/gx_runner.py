"""Great Expectations runner over a local PyIceberg scan (ADR-002: zero Athena spend).

Window: [last successful check minus 5-min overlap, now). First run looks back
1 day. Returns a typed QualityResult; callers exit nonzero on failure so the
scheduler can skip dbt (Brief §6).
"""

import json
import time
import traceback
from datetime import UTC, datetime, timedelta
from pathlib import Path

import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThanOrEqual, Reference
from pyiceberg.expressions.literals import literal
from pyiceberg.table import Table

from pipeline.common.config import Settings
from pipeline.common.iceberg import (
    TICKER_TABLE,
    append_arrow,
    bronze_location,
    ensure_bronze_table,
    ensure_ops_tables,
    load_lake_catalog,
    quality_result_to_arrow,
)
from pipeline.common.logging import get_logger
from pipeline.common.models import QualityResult

logger = get_logger("quality")

QUALITY_DIR = Path("quality")
SUITE_PATH = QUALITY_DIR / "suites" / "bronze_suite.json"
REPORTS_DIR = QUALITY_DIR / "reports"

OVERLAP = timedelta(minutes=5)
FIRST_RUN_LOOKBACK = timedelta(days=1)
CLOCK_SKEW_ALLOWANCE = timedelta(seconds=60)

SUITE_NAME = "bronze_trades"
TICKER_SUITE_NAME = "bronze_tickers"

BRONZE_COLUMNS = [
    "symbol",
    "trade_id",
    "price",
    "quantity",
    "event_time",
    "ingest_time",
    "is_buyer_maker",
]

TICKER_COLUMNS = [
    "symbol",
    "event_time",
    "ingest_time",
    "last_price",
    "last_quantity",
    "bid_price",
    "bid_quantity",
    "ask_price",
    "ask_quantity",
    "open_24h",
    "high_24h",
    "low_24h",
    "volume_24h",
    "quote_volume_24h",
    "trade_count_24h",
]


def _naive(value: datetime) -> datetime:
    """Iceberg/pandas timestamps here are naive UTC; strip tzinfo for comparisons."""
    return value.astimezone(UTC).replace(tzinfo=None)


def _last_success_at(quality_table: Table) -> datetime | None:
    """Latest successful check_at (aware UTC), or None when the table has none."""
    df = quality_table.scan().to_pandas()
    if df.empty or "success" not in df.columns or "check_at" not in df.columns:
        return None
    passed = df[df["success"].astype(bool)]
    if passed.empty:
        return None
    latest: datetime = pd.Timestamp(passed["check_at"].max()).to_pydatetime()
    return latest.replace(tzinfo=UTC)


def resolve_window(
    quality_table: Table, now: datetime
) -> tuple[datetime, datetime]:
    """Scan window: last success minus overlap (first run: 1-day lookback)."""
    last = _last_success_at(quality_table)
    start = (now - FIRST_RUN_LOOKBACK) if last is None else (last - OVERLAP)
    return (min(start, now), now)


def load_window(bronze: Table, start: datetime, end: datetime) -> pd.DataFrame:
    """Load the bronze window. Adds `unique_key` (symbol:trade_id) for the
    composite uniqueness check — trade_id is only unique per symbol (ADR-009)."""
    predicate = And(
        GreaterThanOrEqual(term=Reference("event_time"), value=literal(_naive(start))),
        LessThanOrEqual(term=Reference("event_time"), value=literal(_naive(end))),
    )
    df: pd.DataFrame = bronze.scan(row_filter=predicate).to_arrow().to_pandas()
    if not df.empty:
        df["unique_key"] = df["symbol"].astype(str) + ":" + df["trade_id"].astype(str)
    return df


def load_ticker_window(bronze: Table, start: datetime, end: datetime) -> pd.DataFrame:
    """Load the ticker window, adding `unique_key` = (symbol, event_time)."""
    predicate = And(
        GreaterThanOrEqual(term=Reference("event_time"), value=literal(_naive(start))),
        LessThanOrEqual(term=Reference("event_time"), value=literal(_naive(end))),
    )
    df: pd.DataFrame = bronze.scan(row_filter=predicate).to_arrow().to_pandas()
    if not df.empty:
        df["unique_key"] = df["symbol"].astype(str) + ":" + df["event_time"].astype(str)
    return df


def build_ticker_suite(freshness_cutoff: datetime, now: datetime) -> gx.ExpectationSuite:
    """Ticker gate: keys, positive quote prices, freshness (window stamped per run)."""
    return gx.ExpectationSuite(
        name=TICKER_SUITE_NAME,
        expectations=[
            gxe.ExpectTableColumnsToMatchSet(column_set=TICKER_COLUMNS, exact_match=False),
            *(
                gxe.ExpectColumnValuesToNotBeNull(column=column)
                for column in TICKER_COLUMNS
            ),
            gxe.ExpectColumnValuesToBeUnique(column="unique_key"),
            *(
                gxe.ExpectColumnValuesToBeBetween(
                    column=column, min_value=0, strict_min=True
                )
                for column in (
                    "last_price",
                    "bid_price",
                    "ask_price",
                    "open_24h",
                    "high_24h",
                    "low_24h",
                )
            ),
            gxe.ExpectColumnMaxToBeBetween(
                column="event_time",
                min_value=_naive(freshness_cutoff),
                max_value=_naive(now + CLOCK_SKEW_ALLOWANCE),
            ),
        ],
    )


def build_bronze_suite(
    freshness_cutoff: datetime, now: datetime
) -> gx.ExpectationSuite:
    """Static suite definition (Brief §6). Freshness bounds are stamped per run."""
    return gx.ExpectationSuite(
        name=SUITE_NAME,
        expectations=[
            gxe.ExpectTableColumnsToMatchSet(
                column_set=BRONZE_COLUMNS, exact_match=False
            ),
            *(
                gxe.ExpectColumnValuesToNotBeNull(column=column)
                for column in BRONZE_COLUMNS
            ),
            gxe.ExpectColumnValuesToBeUnique(column="unique_key"),
            gxe.ExpectColumnValuesToBeBetween(
                column="price", min_value=0, strict_min=True
            ),
            gxe.ExpectColumnValuesToBeBetween(
                column="quantity", min_value=0, strict_min=True
            ),
            gxe.ExpectColumnMaxToBeBetween(
                column="event_time",
                min_value=_naive(freshness_cutoff),
                max_value=_naive(now + CLOCK_SKEW_ALLOWANCE),
            ),
            gxe.ExpectTableRowCountToBeBetween(min_value=1),
        ],
    )


def _run_checkpoint(
    df: pd.DataFrame, suite: gx.ExpectationSuite
) -> tuple[bool, list[str]]:
    """Ephemeral GX context over the in-memory dataframe. Returns (ok, failed types)."""
    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_pandas("bronze")
    asset = datasource.add_dataframe_asset(name="bronze_trades")
    batch_definition = asset.add_batch_definition_whole_dataframe("bronze_window")
    context.suites.add(suite)
    validation = gx.ValidationDefinition(
        name="bronze_validation", data=batch_definition, suite=suite
    )
    context.validation_definitions.add(validation)
    checkpoint = gx.Checkpoint(
        name="bronze_checkpoint", validation_definitions=[validation]
    )
    context.checkpoints.add(checkpoint)
    outcome = checkpoint.run(batch_parameters={"dataframe": df})
    failed: list[str] = []
    for validation_result in outcome.run_results.values():
        for result in validation_result.results:
            if not result.success and result.expectation_config is not None:
                failed.append(result.expectation_config.type)
                # Without the observed values a failure is undiagnosable from
                # the structured log ("uniqueness failed" — how many, which?).
                details = result.result or {}
                logger.warning(
                    "expectation_failed",
                    expectation=result.expectation_config.type,
                    column=result.expectation_config.kwargs.get("column"),
                    element_count=details.get("element_count"),
                    unexpected_count=details.get("unexpected_count"),
                    partial_unexpected=details.get("partial_unexpected_list", [])[:3],
                )
    return bool(outcome.success), failed


def _write_report(result: QualityResult, suite: gx.ExpectationSuite) -> Path:
    """Persist the JSON artifact (gitignored) and export the suite snapshot once."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORTS_DIR / result.check_at.strftime("%Y%m%dT%H%M%SZ.json")
    payload = result.model_dump(mode="json")
    payload["suite"] = SUITE_NAME
    path.write_text(json.dumps(payload, indent=2))
    if not SUITE_PATH.exists():
        SUITE_PATH.parent.mkdir(parents=True, exist_ok=True)
        SUITE_PATH.write_text(json.dumps(suite.to_json_dict(), indent=2, default=str))
    return path


def run_quality(
    settings: Settings, warehouse_dir: Path | None = None
) -> QualityResult:
    """Run one GX validation over the bronze window; record result in ops + file."""
    started = time.monotonic()
    check_at = datetime.now(UTC)
    catalog = load_lake_catalog(settings, warehouse_dir)
    bronze = ensure_bronze_table(
        catalog, settings.glue_database, location=bronze_location(settings)
    )
    quality_table = ensure_ops_tables(catalog, settings.glue_database).quality

    window_start, window_end = resolve_window(quality_table, check_at)
    logger.info(
        "quality_window",
        window_start=window_start.isoformat(),
        window_end=window_end.isoformat(),
    )
    df = load_window(bronze, window_start, window_end)

    failed: list[str]
    ticker_failed: list[str] = []
    ticker_table = (
        catalog.load_table(f"{settings.glue_database}.{TICKER_TABLE}")
        if catalog.table_exists(f"{settings.glue_database}.{TICKER_TABLE}")
        else None
    )
    ticker_rows = 0
    if ticker_table is None:
        logger.info("ticker_table_missing_skipped")
    else:
        ticker_df = load_ticker_window(ticker_table, window_start, window_end)
        ticker_rows = int(len(ticker_df))
        if ticker_df.empty:
            ticker_failed = ["ticker:empty_window"]
        else:
            try:
                _, ticker_checks = _run_checkpoint(
                    ticker_df,
                    build_ticker_suite(
                        freshness_cutoff=check_at
                        - timedelta(seconds=settings.freshness_max_lag_s),
                        now=check_at,
                    ),
                )
                ticker_failed = [f"ticker:{name}" for name in ticker_checks]
            except Exception as exc:  # noqa: BLE001 — a GX crash is a failed check
                logger.error(
                    "gx_ticker_runner_error",
                    error=str(exc),
                    traceback=traceback.format_exc(limit=5),
                )
                ticker_failed = [f"ticker:runner_error:{type(exc).__name__}"]

    suite = build_bronze_suite(
        freshness_cutoff=check_at - timedelta(seconds=settings.freshness_max_lag_s),
        now=check_at,
    )
    if df.empty:
        failed = ["empty_window"]
    else:
        try:
            _, failed = _run_checkpoint(df, suite)
        except Exception as exc:  # noqa: BLE001 — a GX crash is a failed check, not a crash
            logger.error("gx_runner_error", error=str(exc), traceback=traceback.format_exc(limit=5))
            failed = [f"runner_error:{type(exc).__name__}"]

    failed.extend(ticker_failed)
    result = QualityResult(
        check_at=check_at,
        success=not failed,
        window_start=window_start,
        window_end=window_end,
        rows_scanned=int(len(df)) + ticker_rows,
        failed_expectations=tuple(failed),
        run_ms=(time.monotonic() - started) * 1000,
    )
    append_arrow(quality_table, quality_result_to_arrow(result))
    report_path = _write_report(result, suite)
    logger.info("quality_checked", report=str(report_path), **result.model_dump(mode="json"))
    return result
