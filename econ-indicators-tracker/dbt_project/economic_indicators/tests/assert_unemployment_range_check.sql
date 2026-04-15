select
    observation_date,
    indicator_name,
    value
from {{ ref('mart_economic_indicators') }}
where series_id = 'UNRATE'
  and (value < 0 or value > 25)