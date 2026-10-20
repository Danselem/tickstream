import io
import zipfile
from datetime import date, datetime
from decimal import Decimal

import pytest

from pipeline.backfill.klines import (
    file_specs,
    file_url,
    parse_kline_zip,
    to_utc,
)

ROWS = "\n".join(
    [
        "1785542400000000,62887.88000000,62900.00000000,62887.87000000,62893.06000000,"
        "7.71179000,1785542459999999,485038.09592460,873,1.18384000,74456.29821520,0",
        "1785542460000000,62893.06000000,62928.00000000,62892.00000000,62928.00000000,"
        "2.76921000,1785542519999999,174181.85632380,1182,1.49271000,93898.00110200,0",
    ]
)


def _zip_csv(text: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("BTCUSDT-1m-2026-08.csv", text)
    return buffer.getvalue()


def test_to_utc_accepts_microseconds_and_milliseconds() -> None:
    assert to_utc(1785542400000000) == datetime(2026, 8, 1)
    assert to_utc(1785542400000) == datetime(2026, 8, 1)


def test_parse_kline_zip_maps_archive_columns() -> None:
    ingest = datetime(2026, 10, 20, 12, 0, 0)
    table = parse_kline_zip(_zip_csv(ROWS), "BTCUSDT", ingest)

    assert table.num_rows == 2
    row = table.to_pylist()[0]
    assert row["symbol"] == "BTCUSDT"
    assert row["open_time"] == datetime(2026, 8, 1)
    assert row["close"] == Decimal("62893.06000000")
    assert row["quote_volume"] == Decimal("485038.09592460")
    assert row["trade_count"] == 873
    assert row["taker_buy_base_volume"] == Decimal("1.18384000")
    assert row["source"] == "archive"


def test_parse_kline_zip_rejects_short_rows() -> None:
    with pytest.raises(ValueError, match=">=11"):
        parse_kline_zip(_zip_csv("1,2,3"), "BTCUSDT", datetime(2026, 10, 20))


def test_file_specs_uses_monthly_for_full_months_then_daily() -> None:
    specs = file_specs(date(2026, 8, 1), date(2026, 10, 19))

    assert specs[:2] == [("monthly", date(2026, 8, 1)), ("monthly", date(2026, 9, 1))]
    assert specs[2] == ("daily", date(2026, 10, 1))
    assert specs[-1] == ("daily", date(2026, 10, 19))
    assert len(specs) == 2 + 19


def test_file_url_monthly_and_daily() -> None:
    assert file_url("monthly", "BTCUSDT", date(2026, 8, 1)).endswith(
        "/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2026-08.zip"
    )
    assert file_url("daily", "BTCUSDT", date(2026, 10, 1)).endswith(
        "/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2026-10-01.zip"
    )
