{{ config(materialized="view") }}

-- Typed, deduped archive klines (one row per symbol + open_time). The archive is
-- static, so a view keeps a single copy; the backfill table is replaced, not merged.
with ranked as (
    select
        symbol,
        open_time,
        open,
        high,
        low,
        close,
        volume,
        quote_volume,
        trade_count,
        taker_buy_base_volume,
        taker_buy_quote_volume,
        ingest_time,
        row_number() over (
            partition by symbol, open_time
            order by ingest_time desc
        ) as _dedup_rank
    from {{ ref("stg_klines") }}
)

select
    symbol,
    open_time,
    open,
    high,
    low,
    close,
    volume,
    quote_volume,
    trade_count,
    taker_buy_base_volume,
    taker_buy_quote_volume,
    ingest_time
from ranked
where _dedup_rank = 1
