-- agg_payment_tip_behaviour: how tipping differs by payment type.
-- Note: TLC only records card tips, so cash tip_amount is (almost) always 0.
SELECT
    f.year_month,
    f.payment_type,
    p.payment_description,
    count(*)                                                    AS trips,
    sum(f.tip_amount)                                           AS total_tips,
    round(avg(f.tip_pct), 2)                                    AS avg_tip_pct,
    round(median(f.tip_pct), 2)                                 AS median_tip_pct,
    round(100.0 * count(*) FILTER (WHERE f.tip_amount > 0) / count(*), 2)
                                                                AS pct_trips_tipped
FROM fact_trips AS f
LEFT JOIN dim_payment_type AS p USING (payment_type)
WHERE f.year_month = {{ year_month }}
GROUP BY ALL
