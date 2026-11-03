-- agg_daily_zone_demand: pickups and revenue per pickup zone per day.
SELECT
    f.year_month,
    f.pickup_date,
    f.pu_location_id,
    z.borough,
    z.zone,
    count(*)                              AS trips,
    sum(f.passenger_count)                AS passengers,
    round(avg(f.trip_distance), 2)        AS avg_trip_distance,
    round(avg(f.trip_duration_min), 2)    AS avg_trip_duration_min,
    sum(f.total_amount)                   AS total_revenue
FROM fact_trips AS f
LEFT JOIN dim_zone AS z ON z.location_id = f.pu_location_id
WHERE f.year_month = {{ year_month }}
GROUP BY ALL
