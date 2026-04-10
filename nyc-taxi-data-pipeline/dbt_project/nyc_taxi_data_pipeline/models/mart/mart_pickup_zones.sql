-- Trip volume by pickup zone and hour of day.

with joined as (

    select * from {{ ref('int_trips_joined') }}

),

aggregated as (

    select
        pickup_borough,
        pickup_zone,
        hour(pickup_datetime) as hour_of_day,
        count(*) as total_trips,
        round(avg(fare_amount), 2) as avg_fare,
        round(avg(trip_distance), 2) as avg_distance_miles,
        round(avg(trip_duration_minutes), 1) as avg_duration_minutes

    from joined
    where pickup_zone is not null
    group by
        pickup_borough,
        pickup_zone,
        hour(pickup_datetime)

)

select * from aggregated
order by total_trips desc