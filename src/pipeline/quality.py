"""Quality: Great Expectations suites at the raw -> staging boundary.

Two suites, with different jobs:

* ``raw_trips`` (observational). Checks on the raw TLC file with ``mostly`` thresholds.
  Bad rows are expected here and quarantine already handles them; the suite tracks how the
  source changes month to month (e.g. the share of missing passenger counts) and never
  blocks a run. A failing expectation is a signal worth reading in Data Docs.
* ``staging_trips`` (contract, blocking). Every validation rule must hold for 100% of
  staged rows, plus checks the rule engine doesn't make (unique keys, derived-column
  ranges). GX evaluates them independently of rules.py, so a bug in either implementation
  fails the run instead of leaking bad rows into the warehouse.

Suites are defined here in code, like rules.py. GX's file context lives under
data/gx/, so validation results and Data Docs accumulate across runs.
"""

from __future__ import annotations

import logging
import warnings
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import duckdb
import great_expectations as gx
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.data_context.types.base import ProgressBarsConfig

from pipeline.config import Config
from pipeline.months import Month
from pipeline.transform import staging_trips_path

log = logging.getLogger(__name__)

DATASOURCE = "pipeline"


class QualityError(RuntimeError):
    """The blocking staging suite failed: staged rows break the data contract."""


@dataclass(frozen=True)
class SuiteResult:
    """Outcome of one suite for one month. Plain types, so Airflow can pass it on."""

    suite: str
    success: bool
    evaluated: int
    successful: int
    failed_expectations: list[str]
    blocking: bool


# Raw columns read for the raw suite (TLC's own names; matched case-insensitively).
RAW_COLUMNS = {
    "pickup": "tpep_pickup_datetime",
    "dropoff": "tpep_dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "fare_amount": "fare_amount",
    "total_amount": "total_amount",
    "pu_location_id": "pulocationid",
    "do_location_id": "dolocationid",
    "payment_type": "payment_type",
}

STAGING_COLUMNS = [
    "trip_key", "year_month", "pickup_datetime", "dropoff_datetime", "passenger_count",
    "trip_distance", "fare_amount", "total_amount", "pu_location_id", "do_location_id",
    "payment_type", "pickup_hour", "pickup_dow", "trip_duration_min",
]  # fmt: skip


def raw_expectations(config: Config, month: Month, zone_ids: list[int]) -> list:
    """Drift checks on the raw file. Thresholds are deliberately loose: they flag change."""
    e, v = gx.expectations, config.validation
    warn = {"severity": "warning"}
    start, end = _bounds(month)
    return [
        e.ExpectTableRowCountToBeBetween(min_value=1, **warn),
        # TLC files contain a handful of stray dates from other months.
        e.ExpectColumnValuesToBeBetween(
            column="pickup", min_value=start, max_value=end, mostly=0.999, **warn
        ),
        e.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="dropoff", column_B="pickup", mostly=0.99, **warn
        ),
        # ~15-25% of 2025 rows are Flex Fare trips without a passenger count.
        e.ExpectColumnValuesToNotBeNull(column="passenger_count", mostly=0.70, **warn),
        e.ExpectColumnValuesToBeBetween(
            column="fare_amount", min_value=0, max_value=v.max_fare_amount, mostly=0.90, **warn
        ),
        e.ExpectColumnValuesToBeBetween(
            column="total_amount", min_value=0, max_value=v.max_total_amount, mostly=0.95, **warn
        ),
        e.ExpectColumnValuesToBeBetween(
            column="trip_distance",
            min_value=0,
            max_value=v.max_trip_distance_miles,
            mostly=0.999,
            **warn,
        ),
        e.ExpectColumnValuesToBeInSet(column="pu_location_id", value_set=zone_ids, **warn),
        e.ExpectColumnValuesToBeInSet(column="do_location_id", value_set=zone_ids, **warn),
        e.ExpectColumnValuesToBeInSet(column="payment_type", value_set=list(range(7)), **warn),
    ]


def staging_expectations(config: Config, month: Month, zone_ids: list[int]) -> list:
    """The data contract for staged rows: everything must hold for every row."""
    e, v = gx.expectations, config.validation
    start, end = _bounds(month)
    not_null = [
        e.ExpectColumnValuesToNotBeNull(column=c)
        for c in ("trip_key", "pickup_datetime", "dropoff_datetime", "passenger_count")
    ]
    return [
        *not_null,
        e.ExpectColumnValuesToBeUnique(column="trip_key"),
        e.ExpectColumnValuesToBeInSet(column="year_month", value_set=[month.key]),
        e.ExpectColumnValuesToBeBetween(column="pickup_datetime", min_value=start, max_value=end),
        e.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="dropoff_datetime", column_B="pickup_datetime"
        ),
        e.ExpectColumnValuesToBeBetween(
            column="trip_duration_min",
            min_value=0,
            max_value=v.max_trip_hours * 60,
            strict_min=True,
            strict_max=True,
        ),
        e.ExpectColumnValuesToBeBetween(column="passenger_count", min_value=1),
        e.ExpectColumnValuesToBeBetween(
            column="fare_amount", min_value=0, max_value=v.max_fare_amount
        ),
        e.ExpectColumnValuesToBeBetween(
            column="total_amount", min_value=0, max_value=v.max_total_amount
        ),
        e.ExpectColumnValuesToBeBetween(
            column="trip_distance", min_value=0, max_value=v.max_trip_distance_miles
        ),
        e.ExpectColumnValuesToBeInSet(column="pu_location_id", value_set=zone_ids),
        e.ExpectColumnValuesToBeInSet(column="do_location_id", value_set=zone_ids),
        e.ExpectColumnValuesToBeBetween(column="pickup_hour", min_value=0, max_value=23),
        e.ExpectColumnValuesToBeBetween(column="pickup_dow", min_value=1, max_value=7),
    ]


def check_quality(
    config: Config, month: Month, raw_path: Path, zones_path: Path
) -> list[SuiteResult]:
    """Run both suites for a month, update Data Docs, and raise if the contract fails."""
    con = duckdb.connect()
    zone_ids = [r[0] for r in con.execute(
        "SELECT DISTINCT CAST(LocationID AS INTEGER) FROM read_csv(?, header = true)",
        [str(zones_path)],
    ).fetchall()]  # fmt: skip

    # Only the checked columns are loaded into pandas, to keep memory per month modest.
    described = con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(raw_path)]).fetchall()
    present = {row[0].lower(): row[0] for row in described}
    raw_select = ", ".join(f'"{present[src]}" AS {alias}' for alias, src in RAW_COLUMNS.items())
    raw_df = con.execute(f"SELECT {raw_select} FROM read_parquet(?)", [str(raw_path)]).df()
    staging = staging_trips_path(config.staging_dir, month)
    staging_df = con.execute(
        f"SELECT {', '.join(STAGING_COLUMNS)} FROM read_parquet(?)", [str(staging)]
    ).df()
    con.close()

    context = _context(config)
    results = [
        _run_suite(context, "raw_trips", raw_expectations(config, month, zone_ids), raw_df, month,
                   blocking=False),
        _run_suite(context, "staging_trips", staging_expectations(config, month, zone_ids),
                   staging_df, month, blocking=True),
    ]  # fmt: skip
    for r in results:
        failed = f" (failed: {', '.join(r.failed_expectations)})" if r.failed_expectations else ""
        log.log(
            logging.INFO if r.success else logging.WARNING,
            "gx %-14s %s: %d/%d expectations passed%s",
            r.suite, month, r.successful, r.evaluated, failed,
        )  # fmt: skip
    log.info("gx data docs: %s", data_docs_index(config))

    contract = results[1]
    if not contract.success:
        raise QualityError(
            f"staging_trips contract failed for {month}: {', '.join(contract.failed_expectations)}"
        )
    return results


def _bounds(month: Month) -> tuple[datetime, datetime]:
    """Month window as datetimes. GX silently fails string bounds against datetime columns
    (no exception, no unexpected count), so the bounds must be real datetime objects."""
    return (
        datetime(month.start.year, month.start.month, 1),
        datetime(month.end.year, month.end.month, 1),
    )


def data_docs_index(config: Config) -> Path:
    """Where GX writes the Data Docs site for this data directory."""
    return (
        config.data_dir.resolve() / "gx" / "uncommitted" / "data_docs" / "local_site" / "index.html"
    )


def _context(config: Config):
    """A file-backed GX context under data/gx/, so results and docs persist across runs."""
    config.data_dir.mkdir(parents=True, exist_ok=True)
    with warnings.catch_warnings():  # GX warns about its own analytics/telemetry defaults
        warnings.simplefilter("ignore")
        context = gx.get_context(mode="file", project_root_dir=str(config.data_dir.resolve()))
    # Progress bars write hundreds of lines per suite into Airflow task logs.
    context.variables.progress_bars = ProgressBarsConfig(globally=False)
    return context


def _run_suite(context, name: str, expectations: list, df, month: Month, *, blocking: bool):
    """(Re)register the suite and its dataframe asset, validate `df`, update Data Docs."""
    source = context.data_sources.add_or_update_pandas(DATASOURCE)
    asset = next((a for a in source.assets if a.name == name), None) or source.add_dataframe_asset(
        name
    )
    batch = next((b for b in asset.batch_definitions if b.name == "whole"), None)
    batch = batch or asset.add_batch_definition_whole_dataframe("whole")

    # Suites are rebuilt from code every run, so this file is their single source of truth.
    suite = context.suites.add_or_update(gx.ExpectationSuite(name=name, expectations=expectations))
    validation = context.validation_definitions.add_or_update(
        gx.ValidationDefinition(name=name, data=batch, suite=suite)
    )
    checkpoint = context.checkpoints.add_or_update(
        gx.Checkpoint(
            name=f"{name}_checkpoint",
            validation_definitions=[validation],
            actions=[gx.checkpoint.UpdateDataDocsAction(name="update_data_docs")],
            result_format="SUMMARY",
        )
    )
    result = checkpoint.run(
        batch_parameters={"dataframe": df}, run_id=RunIdentifier(run_name=f"{name}-{month.key}")
    )
    validation_result = next(iter(result.run_results.values()))
    stats = validation_result.statistics
    failed = [_label(r.expectation_config) for r in validation_result.results if not r.success]
    return SuiteResult(
        suite=name,
        success=bool(validation_result.success),
        evaluated=int(stats["evaluated_expectations"]),
        successful=int(stats["successful_expectations"]),
        failed_expectations=failed,
        blocking=blocking,
    )


def _label(config) -> str:
    """Short readable name for an expectation, e.g. 'not_be_null(passenger_count)'."""
    kwargs = config.kwargs
    name = config.type.removeprefix("expect_column_values_to_").removeprefix("expect_")
    if "column_A" in kwargs:
        return f"{name}({kwargs['column_A']}>{kwargs['column_B']})"
    return f"{name}({kwargs['column']})" if "column" in kwargs else name
