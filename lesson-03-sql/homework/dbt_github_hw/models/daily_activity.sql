with agg_daily_events as (
    SELECT
        event_date,
        count(*) AS events
    from {{ ref('stg_events') }}
    group by event_date
)
SELECT
    event_date,
    events,
    SUM(events) OVER (ORDER BY event_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_events
from agg_daily_events
