with source as (

    select * from {{source('raw', 'raw_zone_lookup')}}
),

casted as (

    select 
        try_cast(locationid as integer) as location_id,
        borough,
        zone,
        service_zone

    from source

)

select * from casted