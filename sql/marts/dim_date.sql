-- dim_date: static calendar covering the full TLC history plus headroom.
SELECT
    CAST(d AS DATE)        AS date_key,
    year(d)                AS year,
    month(d)               AS month,
    day(d)                 AS day,
    isodow(d)              AS day_of_week,
    dayname(d)             AS day_name,
    monthname(d)           AS month_name,
    isodow(d) IN (6, 7)    AS is_weekend
FROM range(DATE '2009-01-01', DATE '2031-01-01', INTERVAL 1 DAY) AS t(d)
