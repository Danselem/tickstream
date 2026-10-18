{{ config(materialized='view') }}

-- Time dimension: a single bounded minute spine derived from the candles'
-- observed time range, with hour/day/weekday attributes for later heatmaps.
-- A view, so it is regenerated per query rather than stored.

with bounds as (
    select
        coalesce(
            date_trunc('minute', min(candle_start)), timestamp '1970-01-01 00:00:00'
        ) as lo,
        coalesce(
            date_trunc('minute', max(candle_start)), timestamp '1970-01-01 00:00:00'
        ) as hi
    from {{ ref('gold_candles_1m') }}
),

spine as (
    select minute_start
    from bounds
    cross join unnest(
        sequence(bounds.lo, bounds.hi, interval '1' minute)
    ) as t(minute_start)
)

select
    minute_start,
    date_trunc('hour', minute_start) as hour_start,
    date_trunc('day', minute_start) as day_start,
    day_of_week(minute_start) as weekday,
    hour(minute_start) as hour_of_day
from spine
