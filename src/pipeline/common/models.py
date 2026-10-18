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


class QualityResult(BaseModel):
    """Outcome of one GX validation run: logs, JSON report, and the ops table."""

    model_config = ConfigDict(frozen=True)

    check_at: datetime
    success: bool
    window_start: datetime
    window_end: datetime
    rows_scanned: int = Field(default=0, ge=0)
    failed_expectations: tuple[str, ...] = ()
    run_ms: float = Field(default=0.0, ge=0)


class IntegrityResult(BaseModel):
    """Outcome of one whole-table bronze integrity check (ADR-016).

    A coarse, low-frequency check: the windowed GX uniqueness gate is frequent
    and cheap, but only sees the recent window; this counts duplicates across the
    whole table so a cross-window duplicate cannot hide.
    """

    model_config = ConfigDict(frozen=True)

    checked_at: datetime
    table_name: str
    total_rows: int = Field(ge=0)
    distinct_keys: int = Field(ge=0)
    duplicates: int = Field(ge=0)
    run_ms: float = Field(default=0.0, ge=0)

    @property
    def ok(self) -> bool:
        return self.duplicates == 0


class LayerCount(BaseModel):
    """Row count of one medallion layer at collection time (ADR-021).

    Bronze -> silver -> gold counts show that nothing is lost between layers.
    """

    model_config = ConfigDict(frozen=True)

    collected_at: datetime
    layer: str = Field(min_length=1)
    row_count: int = Field(ge=0)


class SymbolFreshness(BaseModel):
    """Per-symbol freshness: how far the latest event lags the check time (ADR-021)."""

    model_config = ConfigDict(frozen=True)

    collected_at: datetime
    symbol: str = Field(min_length=1)
    max_event_time: datetime | None = None
    freshness_seconds: float | None = Field(default=None, ge=0)

