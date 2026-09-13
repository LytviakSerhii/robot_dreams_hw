-- Тест: churn у fact_pull_request завжди = additions + deletions.
-- Тест падає, якщо запит поверне рядки.
select *
from {{ ref('fact_pull_request') }}
where churn != (additions + deletions)
