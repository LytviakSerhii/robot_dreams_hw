with all_dates as (
    select to_date(pushed_at) as d from {{ ref('commits') }} where pushed_at is not null
    union all
    select to_date(opened_at) as d from {{ ref('pull_requests') }} where opened_at is not null
    union all
    select to_date(merged_at) as d from {{ ref('pull_requests') }} where merged_at is not null
    union all
    select to_date(opened_at) as d from {{ ref('issues') }} where opened_at is not null
    union all
    select to_date(closed_at) as d from {{ ref('issues') }} where closed_at is not null
),
bounds as (
    select
        min(d) as min_date,
        max(d) as max_date
    from all_dates
),
calendar as (
    select explode(sequence(min_date, max_date, interval 1 day)) as date_day
    from bounds
)
select
    cast(date_format(date_day, 'yyyyMMdd') as int) as date_id,
    date_day,
    dayofweek(date_day) as day_of_week,
    case when dayofweek(date_day) in (1, 7) then true else false end as is_weekend,
    weekofyear(date_day) as iso_week,
    year(date_day) as year
from calendar
