-- Rewrite only candles whose measures actually changed (late trades).
-- IS DISTINCT FROM is null-safe for single-trade candles (volatility NULL).
{{
  config(
    unique_key=["symbol", "candle_start"],
    partitioned_by=["DAY(candle_start)", "symbol"],
    update_condition="(target.open IS DISTINCT FROM src.open OR target.high IS DISTINCT FROM src.high OR target.low IS DISTINCT FROM src.low OR target.close IS DISTINCT FROM src.close OR target.volume IS DISTINCT FROM src.volume OR target.vwap IS DISTINCT FROM src.vwap OR target.trade_count IS DISTINCT FROM src.trade_count OR target.volatility IS DISTINCT FROM src.volatility)",
  )
}}

-- 1-minute OHLC + volume + VWAP + volatility. Incremental MERGE on
-- (symbol, candle_start) with a 30-minute trailing recompute window, so late
-- trades correct their candle on the next run instead of duplicating it.
with window_trades as (
    select symbol, trade_id, event_time, price, quantity
    from {{ ref("silver_trades") }}
    {% if is_incremental() %}
    where event_time >= (
        select
            coalesce(
                date_trunc('minute', max(event_time) - interval '30' minute),
                timestamp '1970-01-01 00:00:00'
            )
        from {{ ref("silver_trades") }}
    )
    {% endif %}
),

numbered as (
    select
        symbol,
        date_trunc('minute', event_time) as candle_start,
        price,
        quantity,
        -- Per-trade log return vs the previous trade (crosses minute boundaries,
        -- so multi-trade candles measure intra-minute price path wiggles).
        ln(price / lag(price) over (
            partition by symbol order by event_time, trade_id
        )) as log_return,
        row_number() over (
            partition by symbol, date_trunc('minute', event_time)
            order by event_time asc, trade_id asc
        ) as rn_open,
        row_number() over (
            partition by symbol, date_trunc('minute', event_time)
            order by event_time desc, trade_id desc
        ) as rn_close
    from window_trades
)

select
    symbol,
    candle_start,
    max(case when rn_open = 1 then price end) as open,
    max(price) as high,
    min(price) as low,
    max(case when rn_close = 1 then price end) as close,
    sum(quantity) as volume,
    sum(price * quantity) / nullif(sum(quantity), 0) as vwap,
    count(*) as trade_count,
    stddev_samp(log_return) as volatility
from numbered
group by symbol, candle_start
