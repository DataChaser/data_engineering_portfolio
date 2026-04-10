-- Daily trip volume and revenue by pickup borough.

with joined as (

    select * from {{ ref('int_trips_joined') }}

),

aggregated as (

    select
        pickup_date,
        pickup_borough,
        count(*) as total_trips,
        round(sum(fare_amount), 2) as total_fare_revenue,
        round(sum(tip_amount), 2) as total_tips,
        round(sum(total_amount), 2) as total_revenue,
        round(avg(fare_amount), 2) as avg_fare,
        round(avg(tip_amount), 2) as avg_tip,
        round(avg(trip_distance), 2) as avg_distance_miles,
        round(avg(trip_duration_minutes), 1) as avg_duration_minutes

    from joined
    where pickup_borough is not null
    group by
        pickup_date,
        pickup_borough

)

select * from aggregated
order by pickup_date, pickup_borough