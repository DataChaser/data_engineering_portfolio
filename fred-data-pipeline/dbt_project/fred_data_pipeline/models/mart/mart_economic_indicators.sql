-- mart model: mart_economic_indicators
-- reads from validated staging model
-- calculates period-on-period change per series at native frequency
-- materialized as a table queried directly by analysts and downstream tools
--
-- change calculation logic:
--  binary series (USREC): null -- 0/1 values, change is meaningless
--  rate/percentage series like unemployment rate, yields etc: percentage point change
--  all other level series: true percentage change

with staging as (select * from {{ ref('stg_fred_observations') }}),

rate_series as (
    select series_id from staging
    where series_id in (
        'UNRATE', 'U6RATE', 'CIVPART', 'FEDFUNDS', 'DFF', 'DGS3MO', 'DGS1', 'DGS2', 'DGS10', 'DGS30',
        'T10Y2Y', 'T10YIE', 'T5YIE', 'DFEDTARL', 'DFEDTARU', 'IORB', 'PSAVERT', 'TCU')
    group by series_id),

binary_series as (
    select series_id from staging
    where series_id in ('USREC')
    group by series_id),

with_changes as (
    select s.observation_date, s.indicator_name, s.series_id, s.frequency, s.value,
    lag(s.value, 1) over (partition by s.series_id order by s.observation_date) as prev_value,
    case
        when b.series_id is not null then null
        when lag(s.value, 1) over (partition by s.series_id order by s.observation_date) is null then null
        when r.series_id is not null then
            round(s.value - lag(s.value, 1) over (partition by s.series_id order by s.observation_date), 2)
        when lag(s.value, 1) over (partition by s.series_id order by s.observation_date) = 0 then null
        else round(((s.value - lag(s.value, 1) over (partition by s.series_id order by s.observation_date)) /
            lag(s.value, 1) over (partition by s.series_id order by s.observation_date)) * 100, 2)
    end as period_on_period_change,
    case
        when b.series_id is not null then 'no change'
        when r.series_id is not null then 'percentage_point_change'
        else 'percentage_change'
    end as change_type

    from staging s
    left join rate_series r on s.series_id = r.series_id
    left join binary_series b on s.series_id = b.series_id)

select
    observation_date, indicator_name, series_id, frequency, value, period_on_period_change, change_type,
    current_timestamp() as dbt_updated_at
from with_changes
order by series_id, observation_date