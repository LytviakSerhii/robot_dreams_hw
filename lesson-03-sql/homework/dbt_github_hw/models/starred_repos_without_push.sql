with starred_repos as (
    SELECT DISTINCT repo_name
    from {{ ref('stg_events') }}
    WHERE event_type = 'WatchEvent'
),
push_repos as (
    SELECT DISTINCT repo_name
    from {{ ref('stg_events') }}
    WHERE event_type = 'PushEvent'
) 
SELECT
    repo_name
from starred_repos
left join push_repos using (repo_name)
where push_repos.repo_name is null