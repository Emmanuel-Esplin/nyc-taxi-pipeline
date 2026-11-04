-- agg_daily_zone_demand: pickups and revenue per pickup zone per day.
select
    f.year_month,
    f.pickup_date,
    f.pu_location_id,
    z.borough,
    z.zone,
    count(*)                              as trips,
    cast(sum(f.passenger_count) as bigint) as passengers,
    round(avg(f.trip_distance), 2)        as avg_trip_distance,
    round(avg(f.trip_duration_min), 2)    as avg_trip_duration_min,
    sum(f.total_amount)                   as total_revenue
from {{ ref('fact_trips') }} as f
left join {{ ref('dim_zone') }} as z on z.location_id = f.pu_location_id
{{ month_filter('f.year_month') }}
group by all
