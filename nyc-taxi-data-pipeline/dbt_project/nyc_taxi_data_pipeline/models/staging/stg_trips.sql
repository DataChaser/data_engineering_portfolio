with source as (

    select * from {{ source('raw', 'raw_trips') }}

),

casted as (

    select
        try_cast(vendorid as integer) as vendor_id,
        try_to_timestamp(tpep_pickup_datetime) as pickup_datetime,
        try_to_timestamp(tpep_dropoff_datetime) as dropoff_datetime,
        try_cast(passenger_count as integer) as passenger_count,
        try_cast(trip_distance as float) as trip_distance,
        try_cast(pulocationid as integer) as pickup_location_id,
        try_cast(dolocationid as integer) as dropoff_location_id,
        try_cast(ratecodeid as integer) as rate_code_id,
        try_cast(payment_type as integer) as payment_type,
        try_cast(fare_amount as float) as fare_amount,
        try_cast(extra as float) as extra,
        try_cast(mta_tax as float) as mta_tax,
        try_cast(tip_amount as float) as tip_amount,
        try_cast(tolls_amount as float) as tolls_amount,
        try_cast(improvement_surcharge as float) as improvement_surcharge,
        try_cast(total_amount as float) as total_amount,
        try_cast(congestion_surcharge as float) as congestion_surcharge,
        try_cast(airport_fee as float) as airport_fee,
        try_cast(cbd_congestion_fee as float) as cbd_congestion_fee,
        store_and_fwd_flag,
        source_month,
        datediff(
            minute,
            try_to_timestamp(tpep_pickup_datetime),
            try_to_timestamp(tpep_dropoff_datetime)
        ) as trip_duration_minutes,
        try_to_timestamp(tpep_pickup_datetime)::date as pickup_date
    from source

),

filtered as (

    select * from casted
    where 
        pickup_datetime is not null
        and dropoff_datetime is not null
        and pickup_datetime < dropoff_datetime
        and pickup_datetime >= '2025-01-01'
        and pickup_datetime < '2026-03-01'
        and fare_amount > 0
        and trip_distance > 0

)

select * from filtered