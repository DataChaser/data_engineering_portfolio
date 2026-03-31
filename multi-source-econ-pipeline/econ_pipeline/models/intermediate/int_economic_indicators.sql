with worldbank as (
    select * from {{ ref('stg_worldbank_indicators') }}
),

wb_pivoted as (
    select
        country_code,
        country_name,
        observation_year,
        max(case when indicator_id = 'NY.GDP.MKTP.CD' then indicator_value end) as gdp_usd,
        max(case when indicator_id = 'FP.CPI.TOTL.ZG' then indicator_value end) as inflation_rate,
        max(case when indicator_id = 'SL.UEM.TOTL.ZS' then indicator_value end) as unemployment_rate
    from worldbank
    group by 1, 2, 3
),

fred as (
    select * from {{ ref('stg_fred_observations') }}
),

fred_annual as (
    select
        year(observation_date) as observation_year,
        max(case when series_id = 'UNRATE' then indicator_value end) as us_unemployment_fred,
        max(case when series_id = 'CPIAUCSL' then indicator_value end) as us_cpi_fred,
        max(case when series_id = 'GNPCA' then indicator_value end) as us_real_gnp_fred
    from fred
    group by 1
),

joined as (
    select
        wb.country_code,
        wb.country_name,
        wb.observation_year,
        wb.gdp_usd,
        wb.inflation_rate,
        wb.unemployment_rate,
        fa.us_unemployment_fred,
        fa.us_cpi_fred,
        fa.us_real_gnp_fred
    from wb_pivoted wb
    left join fred_annual fa
        on wb.observation_year = fa.observation_year
)

select * from joined