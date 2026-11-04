-- dim_zone: one row per TLC taxi zone.
select location_id, borough, zone, service_zone
from {{ ref('stg_zones') }}
