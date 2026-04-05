with source as (
    select * from {{ source('raw', 'fred_observations') }}
),

cleaned as (
    select
        cast(date as date) as observation_date,
        indicator_name,
        series_id,
        cast(value as float) as value,
        loaded_at
    from source
    where date is not null
)

select * from cleaned