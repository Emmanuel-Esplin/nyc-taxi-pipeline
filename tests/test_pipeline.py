"""Integration tests: the full month pipeline on the committed 10k-row fixture."""

from __future__ import annotations

import shutil
from dataclasses import replace
from pathlib import Path

import duckdb
import pytest

from pipeline.config import load_config
from pipeline.dbt import DbtBuildError
from pipeline.months import Month
from pipeline.pipeline import run_month
from pipeline.transform import staging_trips_path
from pipeline.validate import rejected_path

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures"
MONTH = Month(2025, 1)
FIXTURE_ROWS = 10_000


@pytest.fixture
def config(tmp_path):
    base = load_config(ROOT / "config.yaml")
    return replace(
        base,
        data_dir=tmp_path / "data",
        warehouse_path=tmp_path / "data" / "warehouse.duckdb",
        dbt_dir=ROOT / "dbt",
        source_dir=FIXTURES,
    )


def query(config, sql):
    with duckdb.connect(str(config.warehouse_path), read_only=True) as con:
        return con.execute(sql).fetchall()


# Every table dbt builds in the warehouse.
MODELS = [
    "dim_zone",
    "dim_date",
    "dim_payment_type",
    "fact_trips",
    "agg_daily_zone_demand",
    "agg_hourly_revenue",
    "agg_payment_tip_behaviour",
]


def snapshot(config) -> dict[str, list[tuple]]:
    """Every row of every model, in a deterministic order."""
    with duckdb.connect(str(config.warehouse_path), read_only=True) as con:
        return {m: con.execute(f"SELECT * FROM {m} ORDER BY ALL").fetchall() for m in MODELS}


def test_row_counts_reconcile_across_layers(config):
    run = run_month(config, MONTH)

    assert run.status == "success"
    assert run.rows_in == FIXTURE_ROWS
    assert run.rows_clean + run.rows_rejected == FIXTURE_ROWS
    assert query(config, "SELECT count(*) FROM fact_trips") == [(run.rows_clean,)]

    staged = duckdb.execute(
        "SELECT count(*) FROM read_parquet(?)", [str(staging_trips_path(config.staging_dir, MONTH))]
    ).fetchone()[0]
    rejected = duckdb.execute(
        "SELECT count(*), count(*) FILTER (WHERE reject_reason = '') FROM read_parquet(?)",
        [str(rejected_path(config.staging_dir, MONTH))],
    ).fetchone()
    assert staged == run.rows_clean
    assert rejected == (run.rows_rejected, 0)


def test_every_rule_is_exercised_by_the_fixture(config):
    run = run_month(config, MONTH)
    assert all(count > 0 for count in run.failures_by_rule.values()), run.failures_by_rule
    assert query(config, "SELECT sum(rows_failed) FROM run_rejections") == [
        (sum(run.failures_by_rule.values()),)
    ]


def test_trip_keys_are_unique(config):
    run_month(config, MONTH)
    assert query(config, "SELECT count(*) - count(DISTINCT trip_key) FROM fact_trips") == [(0,)]


def test_aggregates_tie_back_to_fact(config):
    run_month(config, MONTH)
    fact = query(config, "SELECT count(*), sum(total_amount) FROM fact_trips")[0]
    for table in ("agg_daily_zone_demand", "agg_hourly_revenue"):
        assert query(config, f"SELECT sum(trips), sum(total_revenue) FROM {table}")[0] == fact
    assert query(config, "SELECT sum(trips) FROM agg_payment_tip_behaviour")[0][0] == fact[0]


def test_rerunning_a_month_is_idempotent(config):
    run_month(config, MONTH)
    first = snapshot(config)
    run_month(config, MONTH)

    assert snapshot(config) == first
    assert query(config, "SELECT count(*) FROM run_log WHERE status = 'success'") == [(2,)]


def test_failed_build_is_logged_and_leaves_data_consistent(config, tmp_path):
    run_month(config, MONTH)
    before = snapshot(config)

    broken_dbt = tmp_path / "dbt"
    shutil.copytree(config.dbt_dir, broken_dbt, ignore=shutil.ignore_patterns("target", "logs"))
    broken_model = broken_dbt / "models" / "aggregates" / "agg_payment_tip_behaviour.sql"
    broken_model.write_text("select nope")
    with pytest.raises(DbtBuildError, match="agg_payment_tip_behaviour"):
        run_month(replace(config, dbt_dir=broken_dbt), MONTH)

    # The failed model keeps its previous table; everything else was rebuilt identically.
    assert snapshot(config) == before
    assert query(config, "SELECT status FROM run_log ORDER BY started_at") == [
        ("success",),
        ("failed",),
    ]
