"""Transform: derive analytics columns for clean rows and write the staging partition."""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb

from pipeline.months import Month
from pipeline.rules import NATURAL_KEY
from pipeline.validate import TAGGED_TABLE

log = logging.getLogger(__name__)


def staging_trips_path(staging_dir: Path, month: Month) -> Path:
    partition = staging_dir / "trips" / f"year={month.year}" / f"month={month.month:02d}"
    return partition / "trips.parquet"


def write_staging(con: duckdb.DuckDBPyConnection, month: Month, path: Path) -> int:
    """Write clean rows with derived columns to the month's staging partition."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.part")
    key_parts = ", ".join(f"coalesce(CAST({col} AS VARCHAR), '')" for col in NATURAL_KEY)
    con.execute(
        f"""
        COPY (
            SELECT
                md5(concat_ws('|', {key_parts})) AS trip_key,
                '{month.key}' AS year_month,
                * EXCLUDE (failed_rules),
                CAST(pickup_datetime AS DATE) AS pickup_date,
                hour(pickup_datetime) AS pickup_hour,
                isodow(pickup_datetime) AS pickup_dow,
                round(epoch(dropoff_datetime - pickup_datetime) / 60.0, 2) AS trip_duration_min,
                CASE WHEN fare_amount > 0
                     THEN round(CAST(tip_amount AS DOUBLE) / CAST(fare_amount AS DOUBLE) * 100, 2)
                END AS tip_pct
            FROM {TAGGED_TABLE}
            WHERE len(failed_rules) = 0
            ORDER BY pickup_datetime, source_row
        ) TO '{tmp}' (FORMAT parquet, COMPRESSION zstd)
        """
    )
    tmp.replace(path)
    rows = con.execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0]
    log.info("staging %s: %s rows -> %s", month, f"{rows:,}", path)
    return rows
