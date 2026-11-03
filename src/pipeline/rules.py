"""Data quality rules applied at the raw -> staging boundary.

Each rule is a SQL boolean expression that is TRUE when a row *fails*. Rules are declared
here in Python (so they are testable and documented in one place) but executed by DuckDB,
so they run vectorised over millions of rows instead of row-by-row in Pandas.

A NULL result counts as a failure: if we cannot prove a row is valid, it is quarantined.
"""

from __future__ import annotations

from dataclasses import dataclass

from pipeline.config import ValidationConfig
from pipeline.months import Month

# Columns that identify a trip. Rows sharing all of them are duplicates; the same columns
# feed the surrogate trip_key, so keys are unique once duplicates are quarantined.
NATURAL_KEY = (
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "pu_location_id",
    "do_location_id",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "total_amount",
    "payment_type",
)


@dataclass(frozen=True)
class Rule:
    """A named check. `failure_sql` is evaluated per row and is TRUE when the row fails."""

    name: str
    description: str
    failure_sql: str


def build_rules(cfg: ValidationConfig, month: Month) -> list[Rule]:
    """Rules for one month's file. Thresholds come from config; the month sets the window.

    Rule order is the order failures are listed in reject_reason.
    """
    start, end = month.start.isoformat(), month.end.isoformat()
    return [
        Rule(
            "pickup_in_file_month",
            f"Pickup timestamp falls inside {month.key}",
            f"NOT (pickup_datetime >= TIMESTAMP '{start}' AND pickup_datetime < TIMESTAMP '{end}')",
        ),
        Rule(
            "dropoff_after_pickup",
            "Dropoff is strictly after pickup",
            "NOT (dropoff_datetime > pickup_datetime)",
        ),
        Rule(
            "trip_duration_plausible",
            f"Trip lasts less than {cfg.max_trip_hours:g} hours",
            f"dropoff_datetime - pickup_datetime >= INTERVAL '{cfg.max_trip_hours} hours'",
        ),
        Rule(
            "fare_amount_in_range",
            f"0 <= fare_amount <= {cfg.max_fare_amount:g}",
            f"NOT (fare_amount BETWEEN 0 AND {cfg.max_fare_amount})",
        ),
        Rule(
            "trip_distance_in_range",
            f"0 <= trip_distance <= {cfg.max_trip_distance_miles:g} miles",
            f"NOT (trip_distance BETWEEN 0 AND {cfg.max_trip_distance_miles})",
        ),
        Rule(
            "total_amount_in_range",
            f"0 <= total_amount <= {cfg.max_total_amount:g}",
            f"NOT (total_amount BETWEEN 0 AND {cfg.max_total_amount})",
        ),
        Rule(
            "passenger_count_positive",
            "passenger_count is present and greater than 0",
            "NOT (passenger_count > 0)",
        ),
        Rule(
            "pickup_zone_known",
            "PULocationID exists in the taxi zone lookup",
            "pu_location_id NOT IN (SELECT location_id FROM zone_lookup)",
        ),
        Rule(
            "dropoff_zone_known",
            "DOLocationID exists in the taxi zone lookup",
            "do_location_id NOT IN (SELECT location_id FROM zone_lookup)",
        ),
        Rule(
            "not_duplicate",
            "First occurrence of its natural key (later copies are quarantined)",
            "_dup_rank > 1",
        ),
    ]
