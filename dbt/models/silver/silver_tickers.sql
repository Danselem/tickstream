-- Snapshots are immutable: keep one row per (symbol, event_time) and only
-- rewrite when a measure differs (the update_condition skips no-ops).
{{
  config(
    unique_key=["symbol", "event_time"],
    partitioned_by=["DAY(event_time)", "symbol"],
    update_condition="(target.last_price IS DISTINCT FROM src.last_price OR target.last_quantity IS DISTINCT FROM src.last_quantity OR target.bid_price IS DISTINCT FROM src.bid_price OR target.bid_quantity IS DISTINCT FROM src.bid_quantity OR target.ask_price IS DISTINCT FROM src.ask_price OR target.ask_quantity IS DISTINCT FROM src.ask_quantity OR target.ingest_time IS DISTINCT FROM src.ingest_time)",
  )
}}

-- Deduped ticker snapshots. Incremental MERGE on (symbol, event_time); the
-- 2-hour lookback absorbs late deliveries and makes re-runs idempotent.
{% set lookback_hours = var("silver_ticker_lookback_hours", 2) %}
with ranked as (
    select
        symbol,
        event_time,
        ingest_time,
        last_price,
        last_quantity,
        bid_price,
        bid_quantity,
        ask_price,
        ask_quantity,
        open_24h,
        high_24h,
        low_24h,
        volume_24h,
        quote_volume_24h,
        trade_count_24h,
        row_number() over (
            partition by symbol, event_time
            order by ingest_time desc
        ) as _dedup_rank
    from {{ ref("stg_tickers") }}
    {% if is_incremental() %}
    where event_time >= (
        select
            coalesce(
                max(event_time) - interval '{{ lookback_hours }}' hour,
                timestamp '1970-01-01 00:00:00'
            )
        from {{ this }}
    )
    {% endif %}
)

select
    symbol,
    event_time,
    ingest_time,
    last_price,
    last_quantity,
    bid_price,
    bid_quantity,
    ask_price,
    ask_quantity,
    open_24h,
    high_24h,
    low_24h,
    volume_24h,
    quote_volume_24h,
    trade_count_24h
from ranked
where _dedup_rank = 1
