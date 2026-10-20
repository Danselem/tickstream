{{ config(materialized="view") }}

-- One candle series for charts: live trade-built candles win where they exist;
-- archive klines fill the rest. `source` tells which path produced the row.
select
    symbol,
    candle_start,
    open,
    high,
    low,
    close,
    volume,
    notional,
    vwap,
    trade_count,
    volatility,
    cast('live' as varchar(5)) as source
from {{ ref("gold_candles_1m") }}

union all

select
    hist.symbol,
    hist.candle_start,
    hist.open,
    hist.high,
    hist.low,
    hist.close,
    hist.volume,
    hist.notional,
    hist.vwap,
    hist.trade_count,
    hist.volatility,
    hist.source
from {{ ref("gold_candles_1m_hist") }} as hist
where not exists (
    select 1
    from {{ ref("gold_candles_1m") }} as live
    where live.symbol = hist.symbol
      and live.candle_start = hist.candle_start
)
