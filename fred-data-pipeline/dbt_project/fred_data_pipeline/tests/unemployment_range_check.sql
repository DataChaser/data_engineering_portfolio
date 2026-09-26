-- unemployment rate should always be between 0 and 25
-- the highest US unemployment on record was ~25% during the Great Depression
-- anything above 25 is almost certainly a data error not a real event, so returning any rows causes this test to fail

select observation_date, series_id, value
from {{ ref('mart_economic_indicators') }}
where series_id = 'UNRATE'
  and value is not null
  and (value < 0 or value > 25)