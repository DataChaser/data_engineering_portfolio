with source as (
    select
        RAW_DATA,
        LOADED_AT
    from {{ source('raw', 'worldbank_raw') }}
),

flattened as (
    select
        RAW_DATA:indicator_code::varchar as indicator_code,
        RAW_DATA:indicator_name::varchar as indicator_name,
        RAW_DATA:extracted_at::varchar as extracted_at,
        LOADED_AT as loaded_at,
        rec.value:countryiso3code::varchar as country_code,
        rec.value:country.value::varchar as country_name,
        rec.value:date::integer as observation_year,
        rec.value:value::float as indicator_value
    from source,
    lateral flatten(input => RAW_DATA:records) rec
)

select * from flattened
where indicator_value is not null