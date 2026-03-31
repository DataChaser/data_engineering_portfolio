with base as (
    select * from {{ ref('int_economic_indicators') }}
),

final as (
    select
        country_code,
        country_name,
        observation_year,
        round(gdp_usd / 1e9, 2) as gdp_billions_usd,
        round(inflation_rate, 2) as inflation_rate_pct,
        round(unemployment_rate, 2) as unemployment_rate_pct,
        round(us_unemployment_fred, 2) as us_unemployment_fred_pct,
        round(us_cpi_fred, 2) as us_cpi_index,
        round(us_real_gnp_fred / 1e3, 2) as us_real_gnp_trillions,
        round(unemployment_rate - us_unemployment_fred, 2) as unemployment_vs_us_delta,
        current_timestamp as dbt_updated_at
    from base
    where observation_year >= 2000
)

select * from final