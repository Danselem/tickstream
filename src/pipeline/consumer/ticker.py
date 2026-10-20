"""Ticker consumer: ticker.raw -> bronze_tickers.

Run with `python -m pipeline.consumer.ticker`.
"""

from pipeline.consumer.run import main

if __name__ == "__main__":
    main("ticker")
