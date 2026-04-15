with staging as (
    select * from {{ ref('stg_fred_observations') }}
),

with_changes as (
    select
        observation_date,
        indicator_name,
        series_id,
        value,
        lag(value, 1) over (partition by indicator_name order by observation_date) as prev_value,
        case
            when series_id in ('UNRATE', 'FEDFUNDS')  --Unemployment and Interest Rates are expressed in percentages, so they are calculated by subtraction. Rest of the indicators are calculated as % changes.
                then round(value - lag(value, 1) over (partition by indicator_name order by observation_date), 2)
            when lag(value, 1) over (partition by indicator_name order by observation_date) is null then null
            when lag(value, 1) over (partition by indicator_name order by observation_date) = 0 then null
            else round(
                ((value - lag(value, 1) over (partition by indicator_name order by observation_date))
                / lag(value, 1) over (partition by indicator_name order by observation_date)) * 100, 2)
        end as mom_change
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
from with_changes
order by indicator_name, observation_date