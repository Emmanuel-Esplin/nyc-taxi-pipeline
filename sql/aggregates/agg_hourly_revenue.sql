-- agg_hourly_revenue: trips and revenue components per pickup date and hour.
SELECT
    year_month,
    pickup_date,
    pickup_hour,
    count(*)                         AS trips,
    sum(fare_amount)                 AS fare_revenue,
    sum(tip_amount)                  AS tip_revenue,
    sum(congestion_surcharge)        AS congestion_revenue,
    sum(total_amount)                AS total_revenue,
    round(avg(total_amount), 2)      AS avg_total_amount
FROM fact_trips
WHERE year_month = {{ year_month }}
GROUP BY ALL
