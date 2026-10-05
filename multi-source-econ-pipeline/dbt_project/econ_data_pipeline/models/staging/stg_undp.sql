with source as (
    select
        RAW_DATA,
        LOADED_AT
    from {{ source('raw', 'undp_raw') }}
),

flattened as (
    select
        RAW_DATA:source::varchar as source,
        RAW_DATA:extracted_at::varchar as extracted_at,
        LOADED_AT as loaded_at,
        split_part(rec.value:country::varchar, ' - ', 1) as country_code,
        split_part(rec.value:country::varchar, ' - ', 2) as country_name,
        split_part(rec.value:indicator::varchar, ' - ', 1) as indicator_code,
        rec.value:indicator::varchar as indicator_full,
        rec.value:year::integer as observation_year,
        rec.value:value::float as indicator_value

    from source,
    lateral flatten(input => RAW_DATA:records) rec
)

select * from flattened
where indicator_value is not null