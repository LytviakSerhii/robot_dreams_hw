-- gold.dim_driver — ЕТАП 2. Grain: водій. SPEC.md, розділ 4.4.
--   * incremental, unique_key='driver_key', delete+insert; за подіями ride_accepted у ref('events')
--   * «останній» рейтинг — за occurred_at; перераховуйте водія з УСІЄЇ його історії, не з батча
--   * плюс член driver_key = 'unknown' (поїздки, скасовані до прийняття)

{{ config(
    materialized='incremental',
    unique_key='driver_key',
    incremental_strategy='delete+insert'
) }}

with acc as (
    select
        payload #>> '{driver,id}' as driver_key,
        payload #>> '{driver,vehicle,type}' as vehicle_type,
        payload #>> '{driver,vehicle,medallion}' as medallion,
        (payload #>> '{driver,rating}')::numeric as rating,
        occurred_at,
        event_id,
        _ingested_at
    from {{ ref('events') }}
    where event_type = 'ride_accepted'
),
{% if is_incremental() %}
touched as (
    select distinct driver_key 
    from acc 
    where _ingested_at > {{ high_watermark() }}
),
{% endif %}
hist as (
    select * 
    from acc
    {% if is_incremental() %} 
    where driver_key in (select driver_key from touched) 
    {% endif %}
),
ranked as (
    select *,
        row_number() over (
            partition by driver_key 
            order by occurred_at desc, event_id desc
        ) as rn,
        min(occurred_at) over (partition by driver_key) as first_seen_at,
        max(occurred_at) over (partition by driver_key) as last_seen_at,
        max(_ingested_at) over (partition by driver_key) as max_ingested
    from hist
)
select
    driver_key,
    vehicle_type,
    medallion,
    rating as latest_rating,
    first_seen_at,
    last_seen_at,
    max_ingested as _ingested_at,
    '{{ run_started_at }}'::timestamptz as _loaded_at
from ranked
where rn = 1
{% if not is_incremental() %}
union all
select 
    'unknown', 'unknown', null, null, null::timestamptz, null::timestamptz,
    null::timestamptz, '{{ run_started_at }}'::timestamptz
{% endif %}
