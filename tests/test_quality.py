"""GX suites: the staging contract blocks bad data; the raw suite only reports."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import duckdb
import pytest

from pipeline import quality
from pipeline.config import load_config
from pipeline.extract import extract
from pipeline.months import Month
from pipeline.pipeline import validate_and_stage
from pipeline.transform import staging_trips_path

ROOT = Path(__file__).resolve().parents[1]
MONTH = Month(2025, 1)


@pytest.fixture
def staged(tmp_path):
    """Config plus raw paths, with the fixture month extracted and staged (no warehouse)."""
    config = replace(
        load_config(ROOT / "config.yaml"),
        data_dir=tmp_path / "data",
        warehouse_path=tmp_path / "data" / "warehouse.duckdb",
        source_dir=ROOT / "tests" / "fixtures",
    )
    raw = extract(config, MONTH)
    validate_and_stage(config, MONTH, raw)
    return config, raw


def test_dirty_raw_is_reported_while_the_staging_contract_passes(staged):
    # The fixture over-samples rare failures (out-of-month pickups, unknown zones, huge
    # distances), so the raw drift suite flags it. Quarantine removed those rows, so the
    # blocking contract on staging passes and the run continues.
    config, raw = staged
    raw_result, contract = quality.check_quality(config, MONTH, raw.trips_path, raw.zones_path)

    assert (raw_result.suite, raw_result.blocking, raw_result.success) == (
        "raw_trips",
        False,
        False,
    )
    assert set(raw_result.failed_expectations) == {
        "be_between(pickup)",
        "be_between(trip_distance)",
        "be_in_set(pu_location_id)",
        "be_in_set(do_location_id)",
    }
    assert (contract.suite, contract.blocking, contract.success) == ("staging_trips", True, True)
    assert contract.evaluated == contract.successful == 17
    assert quality.data_docs_index(config).exists()


def test_contract_violation_in_staging_fails_the_run(staged):
    config, raw = staged
    path = staging_trips_path(config.staging_dir, MONTH)
    # Simulate a rule-engine bug: one zero-passenger trip slips into staging.
    duckdb.execute(
        f"COPY (SELECT * REPLACE (CASE WHEN source_row = (SELECT min(source_row) FROM "
        f"'{path}') THEN 0 ELSE passenger_count END AS passenger_count) FROM '{path}') "
        f"TO '{path}.tmp' (FORMAT parquet)"
    )
    Path(f"{path}.tmp").replace(path)

    with pytest.raises(quality.QualityError, match=r"be_between\(passenger_count\)"):
        quality.check_quality(config, MONTH, raw.trips_path, raw.zones_path)
