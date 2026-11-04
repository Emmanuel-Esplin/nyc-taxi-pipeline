-- fact_trips: one row per validated trip.
-- delete+insert on year_month: a run deletes the months it is loading, then inserts them,
-- so rerunning a month replaces it with identical rows and leaves other months untouched.
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='year_month',
        on_schema_change='fail',
    )
}}

select
    trip_key,
    year_month,
    pickup_date,
    pickup_datetime,
    dropoff_datetime,
    pickup_hour,
    pickup_dow,
    pu_location_id,
    do_location_id,
    payment_type,
    vendor_id,
    rate_code_id,
    passenger_count,
    trip_distance,
    trip_duration_min,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    total_amount,
    tip_pct
from {{ ref('stg_trips') }}
{{ month_filter() }}
