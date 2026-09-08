select
    md5(repo_name) as repo_id,
    repo_name,
    min(repo_owner) as repo_owner,
    min(created_at) as first_seen_at,
    max(created_at) as last_seen_at,
    count(*) as event_count,
    coalesce(max(event_type = 'ForkEvent'), false) as is_forked
from {{ ref('events') }}
group by repo_name
