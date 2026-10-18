{{ config(materialized='view') }}

-- Symbol dimension: one row per trading pair observed in silver, with the
-- base/quote split parsed from the pair name. Cheap view over an aggregate, so
-- it never grows with trade volume.

select
    symbol,
    regexp_replace(symbol, '(USDT|USDC|BUSD|FDUSD|TUSD)$', '') as base_asset,
    regexp_extract(symbol, '(USDT|USDC|BUSD|FDUSD|TUSD)$', 0) as quote_asset,
    min(event_time) as first_seen,
    max(event_time) as last_seen
from {{ ref('silver_trades') }}
group by symbol
