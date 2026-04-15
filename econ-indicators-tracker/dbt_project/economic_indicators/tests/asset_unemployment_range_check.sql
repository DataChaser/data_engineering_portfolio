select
    observation_date,
    indicator_name,
    value
from {{ ref('mart_economic_indicators') }}
where indicator_name = 'Unemployment Rate'
  and (value < 0 or value > 25)