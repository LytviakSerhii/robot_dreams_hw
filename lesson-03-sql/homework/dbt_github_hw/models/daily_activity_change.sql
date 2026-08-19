with agg_daily_events as (
    SELECT
        event_date,
        count(*) AS events
    from {{ ref('stg_events') }}
    group by event_date
),
previous_day_events as (
    SELECT
        event_date,
        events,
        LAG(events) OVER (ORDER BY event_date) AS prev_day_events
    from agg_daily_events
)
SELECT
    event_date,
    events,
    prev_day_events,
    events - prev_day_events  AS delta_events
from previous_day_events