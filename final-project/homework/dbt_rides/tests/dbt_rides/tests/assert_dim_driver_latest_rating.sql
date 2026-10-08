{{ config(tags=['reconcile']) }}
-- Проверяем, что latest_rating в dim_driver совпадает с рейтингом из последнего
-- по времени (occurred_at) события ride_accepted, а не затерт поздним батчем.
with exp as (
    select distinct on (payload #>> '{driver,id}')
        payload #>> '{driver,id}' as driver_key,
        (payload #>> '{driver,rating}')::numeric as rating
    from {{ ref('events') }}
    where event_type = 'ride_accepted'
    order by payload #>> '{driver,id}', occurred_at desc, event_id desc
)
select d.driver_key, d.latest_rating, e.rating
from {{ ref('dim_driver') }} d
full outer join exp e using (driver_key)
where coalesce(d.driver_key, '') <> 'unknown'
  and (d.driver_key is null or e.driver_key is null or d.latest_rating is distinct from e.rating)
