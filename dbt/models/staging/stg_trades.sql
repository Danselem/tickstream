{{ config(materialized="view") }}

-- Thin contract over bronze: types pinned, no business logic.
select
    symbol,
    trade_id,
    cast(price as decimal(20, 8)) as price,
    cast(quantity as decimal(20, 8)) as quantity,
    event_time,
    ingest_time,
    is_buyer_maker
from {{ source("lake", "bronze_trades") }}
