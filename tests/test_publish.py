"""Publishing the serving tables to Postgres.

Runs only when PIPELINE_TEST_POSTGRES_URL points at a scratch database (CI provides one).
It overwrites the serving tables there, so never point it at a real `analytics` database.
"""

from __future__ import annotations

import os
from dataclasses import replace
from pathlib import Path

import duckdb
import pytest

from pipeline.config import load_config
from pipeline.months import Month
from pipeline.pipeline import run_month
from pipeline.publish import SERVING_TABLES, publish

ROOT = Path(__file__).resolve().parents[1]
POSTGRES_URL = os.environ.get("PIPELINE_TEST_POSTGRES_URL")


@pytest.fixture
def config(tmp_path):
    return replace(
        load_config(ROOT / "config.yaml"),
        data_dir=tmp_path / "data",
        warehouse_path=tmp_path / "data" / "warehouse.duckdb",
        source_dir=ROOT / "tests" / "fixtures",
        serving_url=POSTGRES_URL,
    )


def postgres_counts(url: str) -> dict[str, int]:
    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{url}' AS pg (TYPE postgres, READ_ONLY)")
    return {
        t: con.execute(f"SELECT count(*) FROM pg.public.{t}").fetchone()[0] for t in SERVING_TABLES
    }


def test_publish_is_skipped_without_a_serving_url(config):
    assert publish(replace(config, serving_url=None)) is None


@pytest.mark.skipif(not POSTGRES_URL, reason="PIPELINE_TEST_POSTGRES_URL not set")
def test_publish_copies_every_serving_table_and_is_repeatable(config):
    run_month(config, Month(2025, 1))

    first = publish(config)
    assert set(first) == set(SERVING_TABLES)
    assert postgres_counts(POSTGRES_URL) == first

    # A second publish is a full refresh: same rows, no duplicates.
    assert publish(config) == first
    assert postgres_counts(POSTGRES_URL) == first
