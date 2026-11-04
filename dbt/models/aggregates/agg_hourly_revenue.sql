-- agg_hourly_revenue: trips and revenue components per pickup date and hour.
select
    year_month,
    pickup_date,
    pickup_hour,
    count(*)                         as trips,
    sum(fare_amount)                 as fare_revenue,
    sum(tip_amount)                  as tip_revenue,
    sum(congestion_surcharge)        as congestion_revenue,
    sum(total_amount)                as total_revenue,
    round(avg(total_amount), 2)      as avg_total_amount
from {{ ref('fact_trips') }}
{{ month_filter() }}
group by all
