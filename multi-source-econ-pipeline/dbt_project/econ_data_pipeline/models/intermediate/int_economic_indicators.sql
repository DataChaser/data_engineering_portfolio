with worldbank as (
    select
        country_code, observation_year, indicator_code, indicator_value,
        'worldbank' as source
    from {{ ref('stg_worldbank') }}
),

imf as (
    select
        country_code, observation_year, indicator_code, indicator_value,
        'imf' as source
    from {{ ref('stg_imf') }}
),

undp as (
    select
        country_code, observation_year, indicator_code, indicator_value,
        'undp' as source
    from {{ ref('stg_undp') }}
)

select * from worldbank
union all
select * from imf
union all
select * from undp