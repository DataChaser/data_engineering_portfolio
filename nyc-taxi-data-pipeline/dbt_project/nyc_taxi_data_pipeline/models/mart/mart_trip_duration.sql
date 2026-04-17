{{
    config(
        materialized='incremental',
        unique_key=['trip_date', 'pickup_borough'],
        incremental_strategy='merge'
    )
}}

-- Average and median trip duration by borough per day.
-- Median is included alongside average because a small number of very long trips (airport runs, heavy traffic) skews the average significantly. 
-- Median gives a more honest picture of the typical trip.

with joined as (

    select * from {{ ref('int_trips_joined') }}
    {% if is_incremental() %}
    where date_trunc('month', pickup_date) > (
        select max(date_trunc('month', trip_date)) from {{ this }}
    )
    {% endif %}

),

aggregated as (

    select
        pickup_date as trip_date,
        pickup_borough,
        count(*) as total_trips,
        round(avg(trip_duration_minutes), 1) as avg_duration_minutes,
        round(percentile_cont(0.5) within group (order by trip_duration_minutes), 1) as median_duration_minutes,
        round(avg(trip_distance), 2) as avg_distance_miles,
        round(min(trip_duration_minutes), 1) as min_duration_minutes,
        round(max(trip_duration_minutes), 1) as max_duration_minutes
    from joined
    where pickup_borough is not null
      and trip_duration_minutes between 1 and 180  --removes any possibly distorted data like trips below 1 minute or abive 3 hours
    group by
        pickup_date,
        pickup_borough

)

select * from aggregated
order by trip_date, pickup_borough