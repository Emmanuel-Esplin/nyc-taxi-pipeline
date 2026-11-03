"""One test per validation rule, on small hand-made DataFrames."""

from __future__ import annotations

import pandas as pd
import pytest

from pipeline.config import ValidationConfig
from pipeline.months import Month
from pipeline.rules import build_rules


def test_valid_trip_passes_every_rule(tag):
    assert tag([{}]) == [[]]


@pytest.mark.parametrize(
    ("override", "rule"),
    [
        (
            {
                "pickup_datetime": pd.Timestamp("2024-12-31 23:59:59"),
                "dropoff_datetime": pd.Timestamp("2025-01-01 00:10:00"),
            },
            "pickup_in_file_month",
        ),
        (
            {
                "pickup_datetime": pd.Timestamp("2025-02-01 00:00:00"),
                "dropoff_datetime": pd.Timestamp("2025-02-01 00:10:00"),
            },
            "pickup_in_file_month",
        ),
        (
            {
                "pickup_datetime": pd.Timestamp("2009-01-01 00:05:00"),
                "dropoff_datetime": pd.Timestamp("2009-01-01 00:10:00"),
            },
            "pickup_in_file_month",
        ),
        ({"dropoff_datetime": pd.Timestamp("2025-01-15 07:59:00")}, "dropoff_after_pickup"),
        ({"dropoff_datetime": pd.Timestamp("2025-01-15 08:00:00")}, "dropoff_after_pickup"),
        ({"dropoff_datetime": pd.Timestamp("2025-01-16 08:00:00")}, "trip_duration_plausible"),
        ({"fare_amount": -5.0}, "fare_amount_in_range"),
        ({"fare_amount": 5000.0}, "fare_amount_in_range"),
        ({"trip_distance": -1.0}, "trip_distance_in_range"),
        ({"trip_distance": 276423.57}, "trip_distance_in_range"),
        ({"total_amount": -12.5}, "total_amount_in_range"),
        ({"total_amount": 863380.37}, "total_amount_in_range"),
        ({"passenger_count": 0}, "passenger_count_positive"),
        ({"passenger_count": None}, "passenger_count_positive"),
        ({"pu_location_id": 999}, "pickup_zone_known"),
        ({"pu_location_id": None}, "pickup_zone_known"),
        ({"do_location_id": 999}, "dropoff_zone_known"),
    ],
)
def test_each_rule_rejects_its_bad_case(tag, override, rule):
    assert tag([override]) == [[rule]]


def test_month_boundaries_are_start_inclusive_end_exclusive(tag):
    first = {
        "pickup_datetime": pd.Timestamp("2025-01-01 00:00:00"),
        "dropoff_datetime": pd.Timestamp("2025-01-01 00:15:00"),
    }
    last = {
        "pickup_datetime": pd.Timestamp("2025-01-31 23:59:59"),
        "dropoff_datetime": pd.Timestamp("2025-02-01 00:15:00"),
    }
    assert tag([first, last]) == [[], []]


def test_zero_fare_and_distance_are_allowed(tag):
    assert tag([{"fare_amount": 0.0, "trip_distance": 0.0}]) == [[]]


def test_duplicates_keep_first_occurrence_only(tag):
    assert tag([{}, {}, {"vendor_id": 1}]) == [[], ["not_duplicate"], []]


def test_row_reports_every_rule_it_fails(tag):
    failed = tag([{"fare_amount": -1.0, "total_amount": -1.0, "passenger_count": 0}])
    assert failed == [["fare_amount_in_range", "total_amount_in_range", "passenger_count_positive"]]


def test_null_values_fail_rather_than_pass(tag):
    failed = tag([{"fare_amount": None, "dropoff_datetime": None}])[0]
    assert {"fare_amount_in_range", "dropoff_after_pickup"} <= set(failed)


def test_thresholds_come_from_config():
    strict = ValidationConfig(max_fare_amount=50)
    fare_rule = next(
        r for r in build_rules(strict, Month(2025, 1)) if r.name == "fare_amount_in_range"
    )
    assert "BETWEEN 0 AND 50" in fare_rule.failure_sql


def test_rule_names_are_unique():
    names = [rule.name for rule in build_rules(ValidationConfig(), Month(2025, 1))]
    assert len(names) == len(set(names))
