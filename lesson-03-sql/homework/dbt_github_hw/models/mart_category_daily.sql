SELECT
    stg_events.event_date,
    calendar.is_weekend,
    event_categories.category,
    COUNT(*) AS events,
    COUNT(DISTINCT stg_events.repo_name) AS distinct_repos,
    COUNT(DISTINCT stg_events.actor_login) AS distinct_actors
FROM {{ ref('stg_events') }}
JOIN event_categories ON stg_events.event_type = event_categories.event_type
JOIN calendar ON stg_events.event_date = calendar.day
GROUP BY stg_events.event_date, calendar.is_weekend, event_categories.category