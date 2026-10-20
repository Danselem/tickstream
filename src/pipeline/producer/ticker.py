"""Ticker producer entrypoint: Binance @ticker -> Kafka. `python -m pipeline.producer.ticker`."""

import asyncio
import signal
from datetime import UTC, datetime
from typing import Any

from pipeline.common.config import get_settings
from pipeline.common.logging import configure_logging, get_logger
from pipeline.common.models import TickerEvent
from pipeline.producer.binance_ws import stream_tickers
from pipeline.producer.kafka_producer import TickerProducer

logger = get_logger("producer.ticker")


def main() -> None:
    """Run until SIGTERM/SIGINT; flush on shutdown."""
    settings = get_settings()
    configure_logging(settings.log_level)
    producer = TickerProducer(settings)
    producer.ensure_topic()
    logger.info("ticker_producer_started", symbols=settings.symbols, topic=settings.ticker_topic)

    stop = asyncio.Event()

    def _handle_message(payload: dict[str, Any]) -> None:
        try:
            event = TickerEvent.from_binance_payload(payload, datetime.now(UTC))
        except ValueError as exc:
            logger.warning("dropping_malformed_ticker", error=str(exc))
            return
        producer.produce(event)

    async def _run() -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)
        await stream_tickers(settings, _handle_message, stop)
        remaining = producer.flush()
        logger.info("ticker_producer_stopped", undelivered=remaining)

    asyncio.run(_run())


if __name__ == "__main__":
    main()
