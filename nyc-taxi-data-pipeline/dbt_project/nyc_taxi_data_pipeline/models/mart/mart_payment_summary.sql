{{
    config(
        materialized='table'
    )
}}

-- Trip and revenue summary by payment type. Refreshes full table on every run as this is aggregation by type of payment..

with joined as (

    select * from {{ ref('int_trips_joined') }}

),

payment_labeled as (

    select *,
        case payment_type
            when 1 then 'Credit Card'
            when 2 then 'Cash'
            when 3 then 'No Charge'
            when 4 then 'Dispute'
            when 5 then 'Unknown'
            when 6 then 'Voided Trip'
            else 'Other'
        end as payment_label
    from joined

),

aggregated as (

    select
        payment_type,
        payment_label,
        count(*) as total_trips,
        round(sum(total_amount), 2) as total_revenue,
        round(avg(fare_amount), 2) as avg_fare,
        round(avg(tip_amount), 2) as avg_tip,
        round(
            avg(case
                    when fare_amount > 0
                    then (tip_amount / fare_amount) * 100
                    else null
                end), 2
        ) as avg_tip_rate_pct,
        round(avg(trip_distance), 2) as avg_distance_miles,
        round(avg(trip_duration_minutes), 1) as avg_duration_minutes
    from payment_labeled
    group by
        payment_type,
        payment_label

)

select * from aggregated
order by total_trips desc