-- dim_zone: one row per TLC taxi zone. Full refresh from the zone lookup seed.
-- dbt: becomes a seed (taxi_zone_lookup.csv) + this model with {{ ref('taxi_zone_lookup') }}.
SELECT
    CAST(LocationID AS INTEGER) AS location_id,
    Borough                     AS borough,
    Zone                        AS zone,
    service_zone
FROM read_csv({{ zone_lookup }}, header = true)
