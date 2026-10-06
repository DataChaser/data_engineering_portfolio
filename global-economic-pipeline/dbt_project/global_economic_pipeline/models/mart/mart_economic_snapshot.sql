with int_data as (select * from {{ ref('int_economic_indicators') }}),

-- Countries seed: adds country_name and bloc
countries as (
    select country_code, country_name, bloc
    from {{ ref('countries') }}
),

-- Indicators seed: adds indicator_name
indicators as (
    select
        series_id as indicator_code,
        indicator_name, source
    from {{ ref('indicators') }}
),

final as (
    select
        id.country_code,
        c.country_name, c.bloc,
        id.observation_year, id.indicator_code,
        i.indicator_name,
        id.source, id.indicator_value,
        current_timestamp as dbt_updated_at

    from int_data id
    left join countries c on id.country_code = c.country_code
    left join indicators i on id.indicator_code = i.indicator_code
)

select * from final