-- One row per symbol per UTC day: session open/close, range, USD notional,
-- volume, trade count, trades/min and % change. Incremental MERGE on
-- (symbol, window_start) with a 2-day trailing recompute so late trades correct
-- the day instead of duplicating it (see macros/market_summary.sql).
{{
  config(
    unique_key=["symbol", "window_start"],
    partitioned_by=["DAY(window_start)", "symbol"],
    update_condition="(target.open IS DISTINCT FROM src.open OR target.close IS DISTINCT FROM src.close OR target.high IS DISTINCT FROM src.high OR target.low IS DISTINCT FROM src.low OR target.notional IS DISTINCT FROM src.notional OR target.volume IS DISTINCT FROM src.volume OR target.trade_count IS DISTINCT FROM src.trade_count OR target.trades_per_min IS DISTINCT FROM src.trades_per_min OR target.change_pct IS DISTINCT FROM src.change_pct)",
  )
}}

{{ market_summary(grain='day', lookback_amount='2', lookback_unit='day') }}
