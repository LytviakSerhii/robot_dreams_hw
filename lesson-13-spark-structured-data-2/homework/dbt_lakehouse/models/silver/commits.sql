with push_events as (
    select
        event_id,
        repo_name,
        actor_login as pushed_by,
        created_at as pushed_at,
        from_json(payload, '{{ var("push_schema") }}') as p
    from {{ ref('events') }}
    where event_type = 'PushEvent'
),
exploded_commits as (
    select
        c.sha as commit_sha,
        repo_name,
        pushed_by,
        regexp_replace(p.ref, '^refs/heads/', '') as branch,
        c.author.name as author_name,
        c.author.email as author_email,
        c.message as message,
        c.`distinct` as is_distinct,
        pushed_at,
        c.message like 'Merge %' as is_merge_commit,
        split(c.message, '\n')[0] as message_subject,
        length(c.message) as message_length,
        event_id
    from push_events
    lateral view explode(p.commits) as c
    where c.sha is not null
),
deduped as (
    select
        *,
        row_number() over (
            partition by commit_sha
            order by pushed_at asc, event_id asc
        ) as rn
    from exploded_commits
)
select
    commit_sha,
    repo_name,
    pushed_by,
    branch,
    author_name,
    author_email,
    message,
    is_distinct,
    pushed_at,
    is_merge_commit,
    message_subject,
    message_length
from deduped
where rn = 1
