-- Тест: sum(fact_repo_activity_daily.commits) = count(*) з fact_commit.
-- Специфікація: ../../SPEC.md → «Тести». Тест падає, якщо запит поверне рядки.
with rollup as (
    select coalesce(sum(commits), 0L) as total_commits
    from {{ ref('fact_repo_activity_daily') }}
),
facts as (
    select count(*) as total_commits
    from {{ ref('fact_commit') }}
)
select *
from rollup
cross join facts
where rollup.total_commits != facts.total_commits
