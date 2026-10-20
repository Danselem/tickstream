-- Recompute only minutes whose measures changed (late snapshots).
{{
  config(
    unique_key=["symbol", "window_start"],
    partitioned_by=["DAY(window_start)", "symbol"],
    update_condition="(target.avg_spread_bps IS DISTINCT FROM src.avg_spread_bps OR target.avg_mid IS DISTINCT FROM src.avg_mid OR target.avg_bid_qty IS DISTINCT FROM src.avg_bid_qty OR target.avg_ask_qty IS DISTINCT FROM src.avg_ask_qty OR target.avg_imbalance IS DISTINCT FROM src.avg_imbalance OR target.last_price IS DISTINCT FROM src.last_price OR target.tick_count IS DISTINCT FROM src.tick_count)",
  )
}}

-- 1-minute top-of-book microstructure: quoted spread (bps), mid, displayed
-- depth and imbalance, last trade price, snapshot count. Incremental MERGE on
-- (symbol, window_start) with a 30-minute trailing recompute window.
{% set lookback_minutes = var("micro_lookback_minutes", 30) %}
with windowed as (
    select
        symbol,
        event_time,
        ingest_time,
        last_price,
        bid_price,
        bid_quantity,
        ask_price,
        ask_quantity
    from {{ ref("silver_tickers") }}
    {% if is_incremental() %}
    where event_time >= (
        select
            coalesce(
                date_trunc(
                    'minute',
                    max(window_start) - interval '{{ lookback_minutes }}' minute
                ),
                timestamp '1970-01-01 00:00:00'
            )
        from {{ this }}
    )
    {% endif %}
)

select
    symbol,
    date_trunc('minute', event_time) as window_start,
    avg((ask_price - bid_price) / nullif((ask_price + bid_price) / 2, 0) * 10000) as avg_spread_bps,
    avg((ask_price + bid_price) / 2) as avg_mid,
    avg(bid_quantity) as avg_bid_qty,
    avg(ask_quantity) as avg_ask_qty,
    avg(
        coalesce(
            (bid_quantity - ask_quantity) / nullif(bid_quantity + ask_quantity, 0),
            0
        )
    ) as avg_imbalance,
    max_by(last_price, event_time) as last_price,
    count(*) as tick_count,
    max(ingest_time) as last_ingest_time
from windowed
group by 1, 2
