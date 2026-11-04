-- agg_payment_tip_behaviour: how tipping differs by payment type.
-- TLC only records card tips, so cash tip_amount is (almost) always 0.
select
    f.year_month,
    f.payment_type,
    p.payment_description,
    count(*)                                                    as trips,
    sum(f.tip_amount)                                           as total_tips,
    round(avg(f.tip_pct), 2)                                    as avg_tip_pct,
    round(median(f.tip_pct), 2)                                 as median_tip_pct,
    round(100.0 * count(*) filter (where f.tip_amount > 0) / count(*), 2)
                                                                as pct_trips_tipped
from {{ ref('fact_trips') }} as f
left join {{ ref('dim_payment_type') }} as p using (payment_type)
{{ month_filter('f.year_month') }}
group by all
