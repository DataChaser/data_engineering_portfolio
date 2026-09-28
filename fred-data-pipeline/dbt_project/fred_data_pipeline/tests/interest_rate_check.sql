-- federal funds rate should always be between 0 and 30
-- the highest it has ever been was around 20% in the early 1980s
-- anything above 30 is almost certainly a data erro, so returning any rows causes this test to fail

select observation_date, series_id, value
from {{ ref('mart_economic_indicators') }}
where series_id = 'FEDFUNDS'
  and value is not null
  and (value < 0 or value > 30)