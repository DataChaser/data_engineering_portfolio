select *
from {{ ref('stg_trips') }}
where pickup_datetime >= dropoff_datetime