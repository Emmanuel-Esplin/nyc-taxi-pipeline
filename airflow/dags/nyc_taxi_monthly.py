"""Airflow DAG: run the NYC taxi pipeline once per month, with catchup for backfills.

Each DAG run processes the month of its data interval: the run for 2025-01-01 loads
January 2025. Tasks call the same stage functions as the CLI (`pipeline.pipeline`):

    wait_for_source -> extract -> validate_and_stage -> dbt_build -> log_run

Configuration comes from the environment (see docker-compose.yml):
    PIPELINE_START_MONTH  first month to load (default 2025-01); catchup backfills from here
    PIPELINE_END_MONTH    optional last month, to bound a backfill
    PIPELINE_TASK_RETRIES retries per task (default 2; CI sets 0 so failures surface fast)
    PIPELINE_CONFIG       path to config.yaml; PIPELINE_* path overrides also apply
"""

from __future__ import annotations

import os
import uuid
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path

import pendulum
from airflow.sdk import Param, dag, get_current_context, task

from pipeline import pipeline
from pipeline.config import Config, load_config
from pipeline.extract import ExtractResult, extract, source_available
from pipeline.months import Month
from pipeline.runlog import RunRecord

CONFIG_PATH = os.environ.get("PIPELINE_CONFIG", "/opt/pipeline/config.yaml")

# One-slot pool shared by every task that reads or writes data. DuckDB allows one writer, and
# a month's validation needs a few GB of memory, so data tasks must never overlap. A pool
# enforces that across scheduled runs, backfills and manual runs alike (backfills ignore
# the DAG's max_active_runs). Created by `airflow pools set` in docker-compose's init.
POOL = "nyc_taxi_pipeline"


def _month_start(env_var: str, default: str | None) -> pendulum.DateTime | None:
    value = os.environ.get(env_var) or default
    if not value:
        return None
    month = Month.parse(value)
    return pendulum.datetime(month.year, month.month, 1, tz="UTC")


def _config() -> Config:
    return load_config(CONFIG_PATH)


def _month(context) -> Month:
    """The month this run processes: the `month` param if given, else the data interval."""
    if override := context["params"].get("month"):
        return Month.parse(override)
    start = context["data_interval_start"]
    return Month(start.year, start.month)


def _record_failure(context) -> None:
    """on_failure_callback: write a `failed` run_log row, as the CLI does on errors."""
    month = _month(context)
    ti = context["task_instance"]
    # Time the run from extract (as log_run does) if it got that far, else from this task.
    raw = ti.xcom_pull(task_ids="extract_month")
    if raw:
        started = datetime.fromisoformat(raw["started_at"])
    else:
        started = (ti.start_date or datetime.now()).replace(tzinfo=None)
    run = RunRecord(
        run_id=f"airflow:{context['dag_run'].run_id}:{uuid.uuid4().hex[:8]}",
        year_month=month.key,
        started_at=started,
        status="failed",
        finished_at=datetime.now(),
        error=f"{context['task_instance'].task_id}: {context.get('exception')!r}",
    )
    pipeline.log_run(_config(), run)


@dag(
    dag_id="nyc_taxi_monthly",
    description="NYC TLC Yellow Taxi: extract, validate, stage and build the warehouse",
    schedule="@monthly",
    start_date=_month_start("PIPELINE_START_MONTH", "2025-01"),
    end_date=_month_start("PIPELINE_END_MONTH", None),
    catchup=True,  # one run per month since start_date: this is how backfills happen
    # Scheduled runs go one month at a time, in order (the pool guards the tasks themselves).
    max_active_runs=1,
    default_args={
        "pool": POOL,
        "retries": int(os.environ.get("PIPELINE_TASK_RETRIES", "2")),
        "retry_delay": timedelta(minutes=10),
        "on_failure_callback": _record_failure,
    },
    params={
        "month": Param(
            None,
            type=["null", "string"],
            pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
            description="Optional YYYY-MM to (re)process instead of the run's data interval.",
        )
    },
    tags=["nyc-taxi", "duckdb", "dbt"],
    doc_md=__doc__,
)
def nyc_taxi_monthly():
    # TLC publishes each month about two months late. Reschedule mode frees the worker slot
    # between checks, so a run can wait for its file for weeks at no cost.
    @task.sensor(
        poke_interval=timedelta(hours=12).total_seconds(),
        timeout=timedelta(days=90).total_seconds(),
        mode="reschedule",
        retries=0,
        pool="default_pool",  # waiting for TLC must not hold the pipeline's only slot
    )
    def wait_for_source() -> bool:
        return source_available(_config(), _month(get_current_context()))

    @task
    def extract_month() -> dict:
        # run_log times the work from here, not the (possibly weeks-long) wait for the file.
        started_at = datetime.now().isoformat()
        raw = extract(_config(), _month(get_current_context()))
        # XComs carry small JSON values; the data itself stays on the shared volume.
        paths = {"trips_path": str(raw.trips_path), "zones_path": str(raw.zones_path)}
        return {**asdict(raw), **paths, "started_at": started_at}

    @task
    def validate_and_stage(raw: dict) -> dict:
        fields = {k: v for k, v in raw.items() if k != "started_at"}
        raw_result = ExtractResult(
            **{
                **fields,
                "trips_path": Path(fields["trips_path"]),
                "zones_path": Path(fields["zones_path"]),
            }
        )
        result = pipeline.validate_and_stage(_config(), _month(get_current_context()), raw_result)
        return asdict(result)

    @task
    def dbt_build(staged: dict) -> None:
        pipeline.load(_config(), _month(get_current_context()), staged["rows_staged"])

    @task
    def log_run(raw: dict, staged: dict) -> None:
        context = get_current_context()
        run = RunRecord(
            run_id=f"airflow:{context['dag_run'].run_id}:{uuid.uuid4().hex[:8]}",
            year_month=_month(context).key,
            started_at=datetime.fromisoformat(raw["started_at"]),
            status="success",
            finished_at=datetime.now(),
            raw_file=Path(raw["trips_path"]).name,
            raw_bytes=raw["trips_bytes"],
            rows_in=staged["rows_in"],
            rows_clean=staged["rows_clean"],
            rows_rejected=staged["rows_rejected"],
            failures_by_rule=staged["failures_by_rule"],
        )
        pipeline.log_run(_config(), run)

    raw = extract_month()
    staged = validate_and_stage(raw)
    built = dbt_build(staged)
    wait_for_source() >> raw
    built >> log_run(raw, staged)


nyc_taxi_monthly()
