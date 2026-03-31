with source as (
    select * from {{ source('raw', 'fred_observations') }}
),

cleaned as (
    select
        series_id,
        series_name,
        date as observation_date,
        value as indicator_value,
        loaded_at
    from source
    where value is not null
)

select * from cleaned