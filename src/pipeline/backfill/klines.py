"""Binance archive backfill: 1-minute klines into a dedicated bronze table."""

from __future__ import annotations

import csv
import hashlib
import io
import time
import urllib.error
import urllib.request
import zipfile
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.expressions import AlwaysTrue
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    DecimalType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

from pipeline.common.config import Settings
from pipeline.common.iceberg import load_lake_catalog
from pipeline.common.logging import get_logger

logger = get_logger("backfill.klines")

ARCHIVE_BASE = "https://data.binance.vision/data/spot"
KLINE_TABLE = "bronze_klines"
DOWNLOAD_RETRIES = 4
DOWNLOAD_TIMEOUT_S = 60

# Binance spot timestamps moved to microseconds in 2025-01; accept both.
MICROSECOND_CUTOFF = 10**14
PRICE_QUANTUM = Decimal("0.00000001")

KLINE_ARROW_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("open_time", pa.timestamp("us"), nullable=False),
        pa.field("open", pa.decimal128(20, 8), nullable=False),
        pa.field("high", pa.decimal128(20, 8), nullable=False),
        pa.field("low", pa.decimal128(20, 8), nullable=False),
        pa.field("close", pa.decimal128(20, 8), nullable=False),
        pa.field("volume", pa.decimal128(20, 8), nullable=False),
        pa.field("quote_volume", pa.decimal128(20, 8), nullable=False),
        pa.field("trade_count", pa.int64(), nullable=False),
        pa.field("taker_buy_base_volume", pa.decimal128(20, 8), nullable=False),
        pa.field("taker_buy_quote_volume", pa.decimal128(20, 8), nullable=False),
        pa.field("ingest_time", pa.timestamp("us"), nullable=False),
        pa.field("source", pa.string(), nullable=False),
    ]
)


def kline_schema() -> Schema:
    """Bronze kline facts: one row per (symbol, open_time), archive-sourced."""
    return Schema(
        NestedField(1, "symbol", StringType(), required=True),
        NestedField(2, "open_time", TimestampType(), required=True),
        NestedField(3, "open", DecimalType(20, 8), required=True),
        NestedField(4, "high", DecimalType(20, 8), required=True),
        NestedField(5, "low", DecimalType(20, 8), required=True),
        NestedField(6, "close", DecimalType(20, 8), required=True),
        NestedField(7, "volume", DecimalType(20, 8), required=True),
        NestedField(8, "quote_volume", DecimalType(20, 8), required=True),
        NestedField(9, "trade_count", LongType(), required=True),
        NestedField(10, "taker_buy_base_volume", DecimalType(20, 8), required=True),
        NestedField(11, "taker_buy_quote_volume", DecimalType(20, 8), required=True),
        NestedField(12, "ingest_time", TimestampType(), required=True),
        NestedField(13, "source", StringType(), required=True),
    )


def kline_partition_spec() -> PartitionSpec:
    """Partition by symbol only: the table is small and queries filter on time."""
    return PartitionSpec(
        PartitionField(
            source_id=1,
            field_id=1000,
            transform=IdentityTransform(),
            name="symbol",
        )
    )


def ensure_kline_table(catalog: Catalog, database: str, location: str | None) -> Table:
    """Create the backfill table if absent (idempotent)."""
    return catalog.create_table_if_not_exists(
        f"{database}.{KLINE_TABLE}",
        schema=kline_schema(),
        partition_spec=kline_partition_spec(),
        location=location,
        properties={"format-version": "2", "write.format.default": "parquet"},
    )


def to_utc(raw: int) -> datetime:
    """Convert a Binance archive timestamp (ms or us) to naive UTC for Iceberg."""
    divisor = 1_000_000 if raw >= MICROSECOND_CUTOFF else 1_000
    return datetime.fromtimestamp(raw / divisor, tz=UTC).replace(tzinfo=None)


def _decimal(value: str) -> Decimal:
    return Decimal(value).quantize(PRICE_QUANTUM)


def parse_kline_row(symbol: str, row: list[str], ingest_time: datetime) -> dict[str, Any]:
    """Map one headerless archive row to the bronze kline contract."""
    if len(row) < 11:
        raise ValueError(f"{symbol}: expected >=11 kline columns, got {len(row)}")
    return {
        "symbol": symbol,
        "open_time": to_utc(int(row[0])),
        "open": _decimal(row[1]),
        "high": _decimal(row[2]),
        "low": _decimal(row[3]),
        "close": _decimal(row[4]),
        "volume": _decimal(row[5]),
        "quote_volume": _decimal(row[7]),
        "trade_count": int(row[8]),
        "taker_buy_base_volume": _decimal(row[9]),
        "taker_buy_quote_volume": _decimal(row[10]),
        "ingest_time": ingest_time,
        "source": "archive",
    }


def parse_kline_zip(data: bytes, symbol: str, ingest_time: datetime) -> pa.Table:
    """Parse one monthly/daily zip into an Arrow table."""
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        names = [name for name in archive.namelist() if name.endswith(".csv")]
        if len(names) != 1:
            raise ValueError(f"{symbol}: expected one csv in archive, got {names}")
        with archive.open(names[0]) as handle:
            rows = [
                parse_kline_row(symbol, row, ingest_time)
                for row in csv.reader(io.TextIOWrapper(handle, encoding="utf-8"))
                if row
            ]
    return pa.Table.from_pylist(rows, schema=KLINE_ARROW_SCHEMA)


def file_specs(start: date, end: date) -> list[tuple[str, date]]:
    """Pick monthly files for complete months and daily files for the rest."""
    specs: list[tuple[str, date]] = []
    current = start
    while current <= end:
        month_start = current.replace(day=1)
        next_month = (month_start + timedelta(days=32)).replace(day=1)
        month_end = next_month - timedelta(days=1)
        if current == month_start and month_end <= end:
            specs.append(("monthly", month_start))
            current = month_end + timedelta(days=1)
        else:
            specs.append(("daily", current))
            current += timedelta(days=1)
    return specs


def file_url(kind: str, symbol: str, day: date) -> str:
    """Binance archive URL for a monthly or daily 1m kline file."""
    if kind == "monthly":
        name = f"{symbol}-1m-{day:%Y-%m}.zip"
        return f"{ARCHIVE_BASE}/monthly/klines/{symbol}/1m/{name}"
    name = f"{symbol}-1m-{day:%Y-%m-%d}.zip"
    return f"{ARCHIVE_BASE}/daily/klines/{symbol}/1m/{name}"


def fetch(url: str) -> bytes:
    """Download with backoff; raises after the final attempt."""
    last: Exception | None = None
    for attempt in range(DOWNLOAD_RETRIES):
        try:
            with urllib.request.urlopen(url, timeout=DOWNLOAD_TIMEOUT_S) as response:
                return bytes(response.read())
        except (urllib.error.URLError, TimeoutError) as exc:
            last = exc
            time.sleep(1.5**attempt)
    raise RuntimeError(f"download failed after {DOWNLOAD_RETRIES} attempts: {url}") from last


def checksum_of(url: str) -> str:
    """Fetch a ``.CHECKSUM`` sidecar and return its SHA256 hex digest."""
    text = fetch(f"{url}.CHECKSUM").decode().strip()
    digest = text.split()[0] if text else ""
    if len(digest) != 64:
        raise ValueError(f"unexpected checksum file for {url}: {text[:80]!r}")
    return digest.lower()


def load_symbol(symbol: str, start: date, end: date) -> pa.Table:
    """Download, verify and parse every archive file for one symbol."""
    ingest_time = datetime.now(UTC).replace(tzinfo=None)
    tables: list[pa.Table] = []
    for kind, day in file_specs(start, end):
        url = file_url(kind, symbol, day)
        payload = fetch(url)
        expected = checksum_of(url)
        actual = hashlib.sha256(payload).hexdigest()
        if actual != expected:
            raise ValueError(f"checksum mismatch for {url}: {actual} != {expected}")
        table = parse_kline_zip(payload, symbol, ingest_time)
        logger.info(
            "kline_file_loaded", symbol=symbol, file=url.rsplit("/", 1)[-1], rows=table.num_rows
        )
        tables.append(table)
    return pa.concat_tables(tables)


def build_backfill(symbols: list[str], start: date, end: date) -> pa.Table:
    """Concatenate every symbol's klines into one Arrow table."""
    return pa.concat_tables([load_symbol(symbol, start, end) for symbol in symbols])


def write_backfill(settings: Settings, arrow: pa.Table, force: bool) -> int:
    """Append (or replace, with ``force``) the backfill; returns rows written."""
    catalog = load_lake_catalog(settings)
    location = f"s3://{settings.s3_bucket}/bronze/{KLINE_TABLE}"
    table = ensure_kline_table(catalog, settings.glue_database, location)
    table = table.refresh()
    existing = int(table.scan(selected_fields=("symbol",)).to_arrow().num_rows)
    if existing and not force:
        logger.warning(
            "kline_backfill_skipped",
            table=KLINE_TABLE,
            existing_rows=existing,
            hint="re-run with --force to replace the table",
        )
        return 0
    if force and existing:
        table.overwrite(arrow, overwrite_filter=AlwaysTrue())
    else:
        table.append(arrow)
    rows = int(arrow.num_rows)
    logger.info(
        "kline_backfill_written",
        table=KLINE_TABLE,
        rows=rows,
        symbols=len(set(arrow.column("symbol").to_pylist())),
        force=force,
    )
    return rows
