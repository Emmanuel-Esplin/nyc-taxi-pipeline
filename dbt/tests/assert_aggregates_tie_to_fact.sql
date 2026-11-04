-- Every aggregate must account for exactly the trips and revenue in fact_trips, per month.
-- Returns one row per (aggregate, month) that disagrees; dbt fails the test if any exist.
with fact as (
    select year_month, count(*) as trips, sum(total_amount) as revenue
    from {{ ref('fact_trips') }}
    group by all
),
aggs as (
    select 'agg_daily_zone_demand' as agg, year_month, sum(trips) as trips, sum(total_revenue) as revenue
    from {{ ref('agg_daily_zone_demand') }} group by all
    union all
    select 'agg_hourly_revenue', year_month, sum(trips), sum(total_revenue)
    from {{ ref('agg_hourly_revenue') }} group by all
    union all
    select 'agg_payment_tip_behaviour', year_month, sum(trips), null
    from {{ ref('agg_payment_tip_behaviour') }} group by all
)
select a.agg, coalesce(a.year_month, f.year_month) as year_month, a.trips, f.trips as fact_trips
from aggs as a
full outer join fact as f using (year_month)
where a.trips is distinct from f.trips
   or (a.revenue is not null and a.revenue <> f.revenue)
