-- Reusable business summary over 1-minute candles: one row per symbol per
-- window (day or hour). Shared by gold_market_summary_daily/_hourly so the
-- open/close/range/%change definitions live in exactly one place.
--
-- `grain` is a Trino date_trunc unit ('day'/'hour'); `lookback_amount`/`lookback_unit`
-- size the incremental trailing recompute (e.g. '2' 'day') so late trades self-correct.
{% macro market_summary(grain, lookback_amount, lookback_unit) %}
with candles as (
    select *
    from {{ ref('gold_candles_1m') }}
    {% if is_incremental() %}
    where candle_start >= (
        select
            coalesce(
                date_trunc('{{ grain }}', max(window_start))
                    - interval '{{ lookback_amount }}' {{ lookback_unit }},
                timestamp '1970-01-01 00:00:00'
            )
        from {{ this }}
    )
    {% endif %}
)

select
    symbol,
    date_trunc('{{ grain }}', candle_start) as window_start,
    min_by(open, candle_start) as open,
    max_by(close, candle_start) as close,
    max(high) as high,
    min(low) as low,
    sum(notional) as notional,
    sum(volume) as volume,
    sum(trade_count) as trade_count,
    cast(sum(trade_count) as double) / count(*) as trades_per_min,
    (max_by(close, candle_start) - min_by(open, candle_start))
        / nullif(min_by(open, candle_start), 0) as change_pct
from candles
group by symbol, date_trunc('{{ grain }}', candle_start)
{% endmacro %}
