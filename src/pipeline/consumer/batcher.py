"""Time/size micro-batch accumulator. Pure logic — no I/O, fully unit-testable."""

import time

from pipeline.common.models import TradeEvent


class TradeBatcher:
    """Collect events until `max_rows` or `interval_s` elapse. Caller passes `now` (testable)."""

    def __init__(self, max_rows: int, interval_s: float) -> None:
        if max_rows < 1:
            raise ValueError("max_rows must be >= 1")
        if interval_s <= 0:
            raise ValueError("interval_s must be > 0")
        self._max_rows = max_rows
        self._interval_s = interval_s
        self._events: list[TradeEvent] = []
        self._window_start = time.monotonic()

    def add(self, event: TradeEvent) -> None:
        """Append one event to the current window."""
        self._events.append(event)

    def should_flush(self, now: float) -> bool:
        """True when the window is full by size, or by time (only if non-empty)."""
        if len(self._events) >= self._max_rows:
            return True
        return bool(self._events) and now - self._window_start >= self._interval_s

    def drain(self) -> list[TradeEvent]:
        """Take the current window. Does NOT restart the timer — the caller calls
        `start_window` *after* the write/commit so a slow flush cannot leave the
        next window already expired (that caused 1-row batches; Oct 2026)."""
        batch = self._events
        self._events = []
        return batch

    def start_window(self, now: float) -> None:
        """Begin timing a fresh batch window. Call once a flush has completed."""
        self._window_start = now

    def __len__(self) -> int:
        return len(self._events)
