with trips as (

    select * from {{ ref('stg_trips') }}

),

zones as (

    select * from {{ ref('stg_zone_lookup') }}

),

joined as (

    select
        trips.vendor_id,
        trips.pickup_datetime,
        trips.dropoff_datetime,
        trips.pickup_date,
        trips.trip_duration_minutes,
        trips.passenger_count,
        trips.trip_distance,
        trips.pickup_location_id,
        trips.dropoff_location_id,
        trips.rate_code_id,
        trips.payment_type,
        trips.fare_amount,
        trips.extra,
        trips.mta_tax,
        trips.tip_amount,
        trips.tolls_amount,
        trips.improvement_surcharge,
        trips.total_amount,
        trips.congestion_surcharge,
        trips.airport_fee,
        trips.cbd_congestion_fee,
        trips.store_and_fwd_flag,
        trips.source_month,
        pickup_zones.borough  as pickup_borough,
        pickup_zones.zone as pickup_zone,
        pickup_zones.service_zone  as pickup_service_zone,
        dropoff_zones.borough as dropoff_borough,
        dropoff_zones.zone as dropoff_zone,
        dropoff_zones.service_zone as dropoff_service_zone
    from trips
    left join zones as pickup_zones
        on trips.pickup_location_id = pickup_zones.location_id
    left join zones as dropoff_zones
        on trips.dropoff_location_id = dropoff_zones.location_id

)

select * from joined