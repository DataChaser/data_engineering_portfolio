select
    observation_date,
    indicator_name,
    mom_change
from ECONOMIC_INDICATORS.MART.mart_economic_indicators
where mom_change is not null
  and abs(mom_change) > 50