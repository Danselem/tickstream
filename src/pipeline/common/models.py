"""Canonical event models. Pydantic validates everything crossing a boundary."""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


def _ms_to_utc(value: int) -> datetime:
    """Convert Binance millisecond epoch to an aware UTC datetime."""
    return datetime.fromtimestamp(value / 1000, tz=UTC)


class TradeEvent(BaseModel):
    """One normalized trade. Dedup key is (symbol, trade_id)."""

    model_config = ConfigDict(frozen=True, strict=False)

    symbol: str = Field(min_length=1)
    trade_id: int = Field(ge=0)
    price: Decimal = Field(gt=0)
    quantity: Decimal = Field(gt=0)
    event_time: datetime  # exchange time (Binance `T`)
    ingest_time: datetime  # arrival time at the producer (UTC)
    is_buyer_maker: bool

    @field_validator("event_time", "ingest_time")
    @classmethod
    def _must_be_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    @classmethod
    def from_binance_payload(cls, payload: dict[str, Any], ingest_time: datetime) -> "TradeEvent":
        """Parse a raw `@trade` stream message, e.g. {"e":"trade","E":...,"s":"XRPUSDT",
        "t":123,"p":"0.52","q":"10","T":1710000000000,"m":false,"M":true}."""
        try:
            event_time = _ms_to_utc(int(payload["T"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"bad trade time in payload: {payload!r}") from exc
        return cls(
            symbol=str(payload["s"]).upper(),
            trade_id=int(payload["t"]),
            price=Decimal(str(payload["p"])),
            quantity=Decimal(str(payload["q"])),
            event_time=event_time,
            ingest_time=ingest_time,
            is_buyer_maker=bool(payload["m"]),
        )


class BatchReport(BaseModel):
    """Summary of one consumer micro-batch, for logs and the ops metrics table."""

    model_config = ConfigDict(frozen=True)

    rows: int = Field(ge=0)
    max_event_time: datetime | None = None
    lag_messages: int = Field(ge=0)
    commit_ms: float = Field(ge=0)
