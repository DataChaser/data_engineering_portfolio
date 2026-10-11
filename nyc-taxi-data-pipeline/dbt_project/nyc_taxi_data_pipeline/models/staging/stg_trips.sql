with source as (select * from {{ source('raw', 'raw_trips') }}),

renamed as (

    select
        VendorID as vendor_id,
        source_month,
        tpep_pickup_datetime as pickup_datetime,
        tpep_dropoff_datetime as dropoff_datetime,
        DATE(tpep_pickup_datetime) as pickup_date,
        DATETIME_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, MINUTE) as trip_duration_minutes,
        passenger_count, trip_distance,
        RatecodeID as rate_code_id,
        store_and_fwd_flag,
        PULocationID as pickup_location_id,
        DOLocationID as dropoff_location_id,
        payment_type,
        fare_amount, extra, mta_tax, tip_amount, tolls_amount, 
        improvement_surcharge, total_amount, congestion_surcharge,
        Airport_fee as airport_fee,
        cbd_congestion_fee
        
    from source

)

select * from renamed