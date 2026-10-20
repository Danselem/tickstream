{{ config(materialized="view") }}

-- Thin contract over the archive kline table: types pinned, no logic.
select
    symbol,
    open_time,
    cast(open as decimal(20, 8)) as open,
    cast(high as decimal(20, 8)) as high,
    cast(low as decimal(20, 8)) as low,
    cast(close as decimal(20, 8)) as close,
    cast(volume as decimal(20, 8)) as volume,
    cast(quote_volume as decimal(20, 8)) as quote_volume,
    cast(trade_count as bigint) as trade_count,
    cast(taker_buy_base_volume as decimal(20, 8)) as taker_buy_base_volume,
    cast(taker_buy_quote_volume as decimal(20, 8)) as taker_buy_quote_volume,
    ingest_time
from {{ source("lake", "bronze_klines") }}
