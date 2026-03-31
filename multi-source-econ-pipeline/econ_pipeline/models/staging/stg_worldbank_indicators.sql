with source as (
    select * from {{ source('raw', 'worldbank_indicators') }}
),

cleaned as (
    select
        country_code,
        country_name,
        indicator_id,
        indicator_name,
        year                            as observation_year,
        value                           as indicator_value,
        loaded_at
    from source
    where value is not null
)

select * from cleaned