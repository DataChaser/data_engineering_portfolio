with source as (
    select
        RAW_DATA,
        LOADED_AT
    from {{ source('raw', 'imf_raw') }}
),

-- First flatten: one row per country per indicator file
country_level as (
    select
        RAW_DATA:indicator_code::varchar as indicator_code,
        RAW_DATA:indicator_name::varchar as indicator_name,
        RAW_DATA:extracted_at::varchar as extracted_at,
        LOADED_AT as loaded_at,
        country_flat.key::varchar as country_code,
        country_flat.value as year_data
    from source,
    lateral flatten(
        input => RAW_DATA:data:values[RAW_DATA:indicator_code::varchar]
    ) country_flat
),

-- Second flatten: one row per year per country per indicator
year_level as (
    select
        indicator_code, indicator_name, extracted_at, loaded_at, country_code,
        year_flat.key::integer as observation_year,
        year_flat.value::float as indicator_value
    from country_level,
    lateral flatten(input => year_data) year_flat
)

select * from year_level
where indicator_value is not null