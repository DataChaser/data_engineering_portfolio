{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            'field': 'trip_date',
            'data_type': 'date',
            'granularity': 'month'
        },
        cluster_by=['pickup_borough']
    )
}}

with joined as (
    select * from {{ ref('int_trips_joined') }}
    where pickup_borough is not null
      and trip_duration_minutes between 1 and 180
),

aggregated as (

    select
        pickup_date as trip_date,
        pickup_borough,
        count(*) as total_trips,
        round(avg(trip_duration_minutes), 1) as avg_duration_minutes,
        round(avg(trip_distance), 2) as avg_distance_miles,
        round(min(trip_duration_minutes), 1) as min_duration_minutes,
        round(max(trip_duration_minutes), 1) as max_duration_minutes

    from joined
    group by pickup_date, pickup_borough

)
select * from aggregated