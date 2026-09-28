-- staging model: stg_fred_observations
-- reads from the raw FRED observations table, cleans, standardises the data, casts types, filters null dates, passes everything else through
-- materialized as a view

with source as (
    select * from {{ source('raw', 'fred_observations') }}
),

cleaned as (
    select
        cast(date as date) as observation_date, 
        indicator_name, series_id, frequency,
        cast(value as float)        as value,
        loaded_at
    from source
    where date is not null
)

select * from cleaned