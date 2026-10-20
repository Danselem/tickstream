"""Unit tests for the canonical event models (offline, no Kafka/AWS)."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from pipeline.common.models import TickerEvent, TradeEvent

RAW_TRADE = {
    "e": "trade",
    "E": 1710000000123,
    "s": "xrpusdt",  # lowercase on purpose: model must normalize
    "t": 987654321,
    "p": "0.52130",
    "q": "150.5",
    "T": 1710000000000,
    "m": False,
    "M": True,
}

INGEST_TIME = datetime(2026, 1, 1, 0, 0, 5, tzinfo=UTC)


def test_from_binance_payload_maps_all_fields() -> None:
    event = TradeEvent.from_binance_payload(RAW_TRADE, INGEST_TIME)
    assert event.symbol == "XRPUSDT"
    assert event.trade_id == 987654321
    assert event.price == Decimal("0.52130")
    assert event.quantity == Decimal("150.5")
    assert event.event_time == datetime.fromtimestamp(1710000000, tz=UTC)
    assert event.ingest_time == INGEST_TIME
    assert event.is_buyer_maker is False


def test_rejects_non_positive_price() -> None:
    with pytest.raises(ValueError, match="price"):
        TradeEvent.from_binance_payload({**RAW_TRADE, "p": "0"}, INGEST_TIME)


def test_rejects_non_positive_quantity() -> None:
    with pytest.raises(ValueError, match="quantity"):
        TradeEvent.from_binance_payload({**RAW_TRADE, "q": "-1"}, INGEST_TIME)


def test_rejects_unparseable_trade_time() -> None:
    with pytest.raises(ValueError, match="trade time"):
        TradeEvent.from_binance_payload({**RAW_TRADE, "T": "not-a-number"}, INGEST_TIME)


def test_rejects_missing_symbol() -> None:
    payload = dict(RAW_TRADE)
    del payload["s"]
    with pytest.raises((ValueError, KeyError)):
        TradeEvent.from_binance_payload(payload, INGEST_TIME)


def test_naive_datetimes_are_treated_as_utc() -> None:
    event = TradeEvent(
        symbol="XRPUSDT",
        trade_id=1,
        price=Decimal("0.5"),
        quantity=Decimal("1"),
        event_time=datetime(2026, 1, 1),
        ingest_time=datetime(2026, 1, 1, 0, 0, 1),
        is_buyer_maker=True,
    )
    assert event.event_time.tzinfo == UTC
    assert event.ingest_time.tzinfo == UTC


RAW_TICKER = {
    "e": "24hrTicker",
    "E": 1710000000456,
    "s": "xrpusdt",
    "p": "0.0012",
    "P": "0.23",
    "w": "0.521",
    "c": "0.5230",
    "Q": "10.5",
    "b": "0.5229",
    "B": "1200",
    "a": "0.5231",
    "A": "900",
    "o": "0.5218",
    "h": "0.5300",
    "l": "0.5150",
    "v": "125000.5",
    "q": "65123.4",
    "n": 4321,
}


def test_ticker_payload_maps_all_fields() -> None:
    event = TickerEvent.from_binance_payload(RAW_TICKER, INGEST_TIME)
    assert event.symbol == "XRPUSDT"
    assert event.last_price == Decimal("0.5230")
    assert event.bid_price == Decimal("0.5229")
    assert event.ask_price == Decimal("0.5231")
    assert event.ask_quantity == Decimal("900")
    assert event.trade_count_24h == 4321
    assert event.event_time == datetime.fromtimestamp(1710000000.456, tz=UTC)


def test_ticker_payload_unwraps_combined_stream() -> None:
    event = TickerEvent.from_binance_payload(
        {"stream": "xrpusdt@ticker", "data": RAW_TICKER}, INGEST_TIME
    )
    assert event.symbol == "XRPUSDT"


def test_ticker_rejects_non_positive_bid() -> None:
    with pytest.raises(ValueError, match="bid_price"):
        TickerEvent.from_binance_payload({**RAW_TICKER, "b": "0"}, INGEST_TIME)
