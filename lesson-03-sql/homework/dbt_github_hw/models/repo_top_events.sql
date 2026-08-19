with agg_repo_events as (
    SELECT
        event_type,
        repo_name,
        count(*) AS event_count
    from {{ ref('stg_events') }}
    group by event_type, repo_name
)
select 
    event_type,
    repo_name,
    event_count,
    ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_count DESC, repo_name) as type_rank 
from agg_repo_events
QUALIFY type_rank <= 5