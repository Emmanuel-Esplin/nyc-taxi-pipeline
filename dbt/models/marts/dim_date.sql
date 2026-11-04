-- dim_date: static calendar covering the full TLC history plus headroom.
select
    cast(d as date)        as date_key,
    year(d)                as year,
    month(d)               as month,
    day(d)                 as day,
    isodow(d)              as day_of_week,
    dayname(d)             as day_name,
    monthname(d)           as month_name,
    isodow(d) in (6, 7)    as is_weekend
from range(date '2009-01-01', date '2031-01-01', interval 1 day) as t(d)
