with commits_daily as (
    select
        repo_name,
        to_date(from_utc_timestamp(pushed_at, 'Europe/Kyiv')) as activity_date,
        count(*) as commits,
        count(distinct author_email) as distinct_committers
    from {{ ref('commits') }}
    where pushed_at is not null
    group by repo_name, to_date(from_utc_timestamp(pushed_at, 'Europe/Kyiv'))
),
prs_opened_daily as (
    select
        repo_name,
        to_date(from_utc_timestamp(opened_at, 'Europe/Kyiv')) as activity_date,
        count(*) as prs_opened
    from {{ ref('pull_requests') }}
    where opened_at is not null
    group by repo_name, to_date(from_utc_timestamp(opened_at, 'Europe/Kyiv'))
),
prs_merged_daily as (
    select
        repo_name,
        to_date(from_utc_timestamp(merged_at, 'Europe/Kyiv')) as activity_date,
        count(*) as prs_merged
    from {{ ref('pull_requests') }}
    where merged_at is not null
    group by repo_name, to_date(from_utc_timestamp(merged_at, 'Europe/Kyiv'))
),
prs_daily as (
    select
        coalesce(o.repo_name, m.repo_name) as repo_name,
        coalesce(o.activity_date, m.activity_date) as activity_date,
        coalesce(o.prs_opened, 0L) as prs_opened,
        coalesce(m.prs_merged, 0L) as prs_merged
    from prs_opened_daily o
    full outer join prs_merged_daily m
        on o.repo_name = m.repo_name
       and o.activity_date = m.activity_date
),
issues_opened_daily as (
    select
        repo_name,
        to_date(from_utc_timestamp(opened_at, 'Europe/Kyiv')) as activity_date,
        count(*) as issues_opened
    from {{ ref('issues') }}
    where opened_at is not null
    group by repo_name, to_date(from_utc_timestamp(opened_at, 'Europe/Kyiv'))
),
issues_closed_daily as (
    select
        repo_name,
        to_date(from_utc_timestamp(closed_at, 'Europe/Kyiv')) as activity_date,
        count(*) as issues_closed
    from {{ ref('issues') }}
    where closed_at is not null
    group by repo_name, to_date(from_utc_timestamp(closed_at, 'Europe/Kyiv'))
),

issues_daily as (
    select
        coalesce(o.repo_name, c.repo_name) as repo_name,
        coalesce(o.activity_date, c.activity_date) as activity_date,
        coalesce(o.issues_opened, 0L) as issues_opened,
        coalesce(c.issues_closed, 0L) as issues_closed
    from issues_opened_daily o
    full outer join issues_closed_daily c
        on o.repo_name = c.repo_name
       and o.activity_date = c.activity_date
),

events_daily as (
    select
        repo_name,
        to_date(from_utc_timestamp(created_at, 'Europe/Kyiv')) as activity_date,
        count(case when event_type = 'WatchEvent' then 1 end) as stars,
        count(case when event_type = 'ForkEvent' then 1 end) as forks
    from {{ ref('events') }}
    where event_type in ('WatchEvent', 'ForkEvent')
      and created_at is not null
    group by repo_name, to_date(from_utc_timestamp(created_at, 'Europe/Kyiv'))
),

joined as (
    select
        repo_name,
        activity_date,
        sum(commits) as commits,
        sum(distinct_committers) as distinct_committers,
        sum(prs_opened) as prs_opened,
        sum(prs_merged) as prs_merged,
        sum(issues_opened) as issues_opened,
        sum(issues_closed) as issues_closed,
        sum(stars) as stars,
        sum(forks) as forks
    from (
        select repo_name, activity_date, commits, distinct_committers, 0L as prs_opened, 0L as prs_merged, 0L as issues_opened, 0L as issues_closed, 0L as stars, 0L as forks
        from commits_daily
        union all
        select repo_name, activity_date, 0L, 0L, prs_opened, prs_merged, 0L, 0L, 0L, 0L
        from prs_daily
        union all
        select repo_name, activity_date, 0L, 0L, 0L, 0L, issues_opened, issues_closed, 0L, 0L
        from issues_daily
        union all
        select repo_name, activity_date, 0L, 0L, 0L, 0L, 0L, 0L, stars, forks
        from events_daily
    )
    group by repo_name, activity_date
)

select
    md5(concat_ws('|', md5(repo_name), cast(date_format(activity_date, 'yyyyMMdd') as string))) as activity_id,
    md5(repo_name) as repo_id,
    cast(date_format(activity_date, 'yyyyMMdd') as int) as date_id,
    commits,
    distinct_committers,
    prs_opened,
    prs_merged,
    issues_opened,
    issues_closed,
    stars,
    forks
from joined
where repo_name is not null 
  and activity_date is not null 
