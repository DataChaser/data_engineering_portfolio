with trips as (select * from {{ ref('stg_trips') }}),

zones as (select * from {{ ref('stg_zone_lookup') }}),

joined as (

    select

        t.vendor_id,
        t.source_month,
        t.pickup_datetime,
        t.dropoff_datetime,
        t.pickup_date,
        t.trip_duration_minutes,
        t.passenger_count,
        t.trip_distance,
        t.rate_code_id,
        t.payment_type,
        t.fare_amount,
        t.tip_amount,
        t.total_amount,
        t.congestion_surcharge,
        t.cbd_congestion_fee,
        t.pickup_location_id,
        p.borough as pickup_borough,
        p.zone as pickup_zone,
        p.service_zone as pickup_service_zone,

        -- Dropoff location — second join on dropoff_location_id
        t.dropoff_location_id,
        d.borough as dropoff_borough,
        d.zone as dropoff_zone,
        d.service_zone as dropoff_service_zone

    from trips t

    left join zones p
        on t.pickup_location_id = p.location_id

    left join zones d
        on t.dropoff_location_id = d.location_id

    where t.fare_amount >= 0
      and t.trip_distance >= 0
      and t.trip_duration_minutes >= 0
      and t.pickup_datetime < t.dropoff_datetime

)

select * from joined