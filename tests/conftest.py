from __future__ import annotations

import duckdb
import pandas as pd
import pytest

from pipeline.config import ValidationConfig
from pipeline.months import Month
from pipeline.rules import build_rules
from pipeline.validate import TAGGED_TABLE, apply_rules

MONTH = Month(2025, 1)

# A trip that passes every rule; tests override one field at a time.
VALID_TRIP = {
    "vendor_id": 2,
    "pickup_datetime": pd.Timestamp("2025-01-15 08:00:00"),
    "dropoff_datetime": pd.Timestamp("2025-01-15 08:20:00"),
    "passenger_count": 1,
    "trip_distance": 3.2,
    "rate_code_id": 1,
    "store_and_fwd_flag": "N",
    "pu_location_id": 161,
    "do_location_id": 236,
    "payment_type": 1,
    "fare_amount": 18.4,
    "tip_amount": 4.0,
    "total_amount": 27.9,
}

ZONES = pd.DataFrame(
    {
        "location_id": [161, 236, 132],
        "borough": ["Manhattan", "Manhattan", "Queens"],
        "zone": ["Midtown Center", "Upper East Side North", "JFK Airport"],
        "service_zone": ["Yellow Zone", "Yellow Zone", "Airports"],
    }
)


def trips_frame(rows: list[dict]) -> pd.DataFrame:
    frame = pd.DataFrame([{**VALID_TRIP, **row} for row in rows])
    frame.insert(0, "source_row", range(len(frame)))
    return frame.astype({"passenger_count": "Int64", "pu_location_id": "Int64"})


@pytest.fixture
def tag():
    """Return a function: list of row overrides -> list of failed_rules per row."""
    rules = build_rules(ValidationConfig(), MONTH)

    def _tag(rows: list[dict]) -> list[list[str]]:
        con = duckdb.connect()
        con.register("zones_df", ZONES)
        con.execute("CREATE TEMP TABLE zone_lookup AS SELECT * FROM zones_df")
        con.register("trips", trips_frame(rows))
        apply_rules(con, "trips", rules)
        query = f"SELECT failed_rules FROM {TAGGED_TABLE} ORDER BY source_row"
        return [row[0] for row in con.execute(query).fetchall()]

    return _tag
