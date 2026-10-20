{{ config(materialized="view") }}

-- Thin contract over bronze: types pinned, no business logic.
select
    symbol,
    event_time,
    ingest_time,
    cast(last_price as decimal(20, 8)) as last_price,
    cast(last_quantity as decimal(20, 8)) as last_quantity,
    cast(bid_price as decimal(20, 8)) as bid_price,
    cast(bid_quantity as decimal(20, 8)) as bid_quantity,
    cast(ask_price as decimal(20, 8)) as ask_price,
    cast(ask_quantity as decimal(20, 8)) as ask_quantity,
    cast(open_24h as decimal(20, 8)) as open_24h,
    cast(high_24h as decimal(20, 8)) as high_24h,
    cast(low_24h as decimal(20, 8)) as low_24h,
    cast(volume_24h as decimal(38, 8)) as volume_24h,
    cast(quote_volume_24h as decimal(38, 8)) as quote_volume_24h,
    trade_count_24h
from {{ source("lake", "bronze_tickers") }}
