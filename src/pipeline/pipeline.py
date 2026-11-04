"""Orchestrate one month: extract -> validate -> transform -> dbt build -> run_log."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime

import duckdb

from pipeline.config import Config
from pipeline.dbt import dbt_build
from pipeline.extract import extract
from pipeline.months import Month
from pipeline.rules import build_rules
from pipeline.runlog import RunRecord, record_run
from pipeline.schema import TYPED_VIEW, create_typed_view, load_zone_lookup
from pipeline.transform import staging_trips_path, write_staging
from pipeline.validate import rejected_path, validate

log = logging.getLogger(__name__)


class ReconciliationError(RuntimeError):
    """Row counts between layers do not add up."""


def run_month(config: Config, month: Month) -> RunRecord:
    """Process one month end to end and return its run_log record.

    Raises on any failure, after recording a ``failed`` row in run_log. Each dbt model
    replaces its month atomically, and the inputs are deterministic, so recovering from a
    failed run is simply rerunning the month.
    """
    run = RunRecord(run_id=str(uuid.uuid4()), year_month=month.key, started_at=datetime.now())
    log.info("=== run %s for %s ===", run.run_id[:8], month)
    try:
        # 1. Extract: land the raw file (skipped when an identical copy is already present).
        raw = extract(config, month)
        run.raw_file, run.raw_bytes = raw.trips_path.name, raw.trips_bytes

        # 2-3. Validate and transform in a throwaway in-memory DuckDB, so the warehouse is
        # only opened once staging is written and the load can be a short transaction.
        with duckdb.connect() as work:
            load_zone_lookup(work, raw.zones_path)
            create_typed_view(work, raw.trips_path)
            rules = build_rules(config.validation, month)
            result = validate(work, TYPED_VIEW, rules, rejected_path(config.staging_dir, month))
            staging_path = staging_trips_path(config.staging_dir, month)
            staged = write_staging(work, month, staging_path)
        # Every row must be accounted for: nothing lost between raw, validation and staging.
        _reconcile("raw footer vs rows read", raw.trips_rows, result.rows_in)
        _reconcile("clean vs staged", result.rows_clean, staged)
        run.rows_in, run.rows_clean = result.rows_in, result.rows_clean
        run.rows_rejected, run.failures_by_rule = result.rows_rejected, result.failures_by_rule

        # 4. Model: dbt builds dimensions, fact and aggregates and runs the data tests.
        dbt_build(config, month)

        # 5. Reconcile the load, then record success.
        with _warehouse(config) as wh:
            loaded = wh.execute(
                "SELECT count(*) FROM fact_trips WHERE year_month = ?", [month.key]
            ).fetchone()[0]
            _reconcile("staged vs fact_trips", staged, loaded)
            run.status, run.finished_at = "success", datetime.now()
            record_run(wh, run)
    except Exception as exc:
        # Record the failure so run_log keeps the history of every attempt.
        run.status, run.finished_at, run.error = "failed", datetime.now(), repr(exc)
        with _warehouse(config) as wh:
            record_run(wh, run)
        log.exception("run for %s failed", month)
        raise
    log.info("=== %s done in %.1fs ===", month, run.duration_seconds)
    return run


def _reconcile(check: str, expected: int, actual: int) -> None:
    """Fail the run if two layers disagree on row count."""
    if expected != actual:
        raise ReconciliationError(f"{check}: expected {expected:,} rows, got {actual:,}")


def _warehouse(config: Config) -> duckdb.DuckDBPyConnection:
    """Open the warehouse file, creating its directory on first use."""
    config.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(config.warehouse_path))
