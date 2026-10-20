{{ config(materialized="view") }}

-- Historical candles from the archive. Same contract as gold_candles_1m, except
-- volatility (per-trade log-return stddev) cannot be recovered from klines: NULL.
-- source='kline' lets the unified view prefer live candles for overlapping minutes.
select
    symbol,
    open_time as candle_start,
    open,
    high,
    low,
    close,
    volume,
    quote_volume as notional,
    cast(quote_volume / nullif(volume, 0) as decimal(20, 8)) as vwap,
    trade_count,
    cast(null as double) as volatility,
    cast('kline' as varchar(5)) as source
from {{ ref("silver_klines") }}
