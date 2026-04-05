with staging as (
    select * from {{ ref('stg_fred_observations') }}
),

with_mom_change as (
    select
        observation_date,
        indicator_name,
        series_id,
        value,
        lag(value) over (
            partition by indicator_name
            order by observation_date
        ) as prev_value,
        value - lag(value) over (
            partition by indicator_name
            order by observation_date
        ) as mom_change
    from staging
)

select
    observation_date,
    indicator_name,
    series_id,
    value,
    prev_value,
    mom_change,
    current_timestamp() as dbt_updated_at
from with_mom_change
order by indicator_name, observation_date