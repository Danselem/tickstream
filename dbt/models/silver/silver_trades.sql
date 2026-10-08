-- Trade facts are immutable: the update_condition skips the rewrite when nothing
-- changed, so the 2-hour lookback inserts new keys without touching existing
-- rows. This keeps every incremental commit tiny (Glue times out on rewrites).
{{
  config(
    unique_key=["symbol", "trade_id"],
    partitioned_by=["DAY(event_time)", "symbol"],
    update_condition="(target.price IS DISTINCT FROM src.price OR target.quantity IS DISTINCT FROM src.quantity OR target.event_time IS DISTINCT FROM src.event_time OR target.ingest_time IS DISTINCT FROM src.ingest_time OR target.is_buyer_maker IS DISTINCT FROM src.is_buyer_maker)",
  )
}}

-- Deduped, typed trades. Incremental MERGE on (symbol, trade_id); the 2-hour
-- lookback absorbs late arrivals and makes re-runs idempotent.
with ranked as (
    select
        symbol,
        trade_id,
        price,
        quantity,
        event_time,
        ingest_time,
        is_buyer_maker,
        row_number() over (
            partition by symbol, trade_id
            order by ingest_time desc
        ) as _dedup_rank
    from {{ ref("stg_trades") }}
    {% if is_incremental() %}
    where event_time >= (
        select
            coalesce(
                max(event_time) - interval '2' hour,
                timestamp '1970-01-01 00:00:00'
            )
        from {{ this }}
    )
    {% endif %}
)

select
    symbol,
    trade_id,
    price,
    quantity,
    event_time,
    ingest_time,
    is_buyer_maker
from ranked
where _dedup_rank = 1
