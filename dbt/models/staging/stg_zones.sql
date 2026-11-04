-- Taxi zones with snake_case columns.
select
    cast(LocationID as integer) as location_id,
    Borough                     as borough,
    Zone                        as zone,
    service_zone
from {{ source('pipeline', 'taxi_zone_lookup') }}
