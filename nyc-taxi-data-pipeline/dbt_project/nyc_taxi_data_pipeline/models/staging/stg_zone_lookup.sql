with source as (select * from {{ source('raw', 'raw_zone_lookup') }}),

renamed as (
    
    select
        CAST(LocationID as INT64) as location_id,
        Borough as borough,
        Zone as zone,
        service_zone

    from source

)

select * from renamed