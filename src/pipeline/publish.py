"""Publish: copy the serving tables from DuckDB into Postgres for BI tools.

DuckDB allows one writer at a time, and a writer locks out other processes entirely, so a
BI tool holding the warehouse file open would block the pipeline (and vice versa). Instead,
after each load the small serving tables (dimensions, aggregates and the audit tables) are
copied into a Postgres `analytics` database, which Metabase reads through a read-only user.

Each publish is a full refresh inside one Postgres transaction: readers see either the old
tables or the new ones, never a half-published state. Publishing is skipped when
PIPELINE_SERVING_URL is not set (local CLI runs, CI, tests).
"""

from __future__ import annotations

import logging

import duckdb

from pipeline.config import Config

log = logging.getLogger(__name__)

# Everything a dashboard needs. fact_trips (millions of rows) stays in DuckDB.
SERVING_TABLES = [
    "dim_zone",
    "dim_payment_type",
    "agg_daily_zone_demand",
    "agg_hourly_revenue",
    "agg_payment_tip_behaviour",
    "run_log",
    "run_rejections",
    "quality_results",
]


def publish(config: Config) -> dict[str, int] | None:
    """Refresh every serving table in Postgres. Returns rows per table, or None if skipped."""
    if not config.serving_url:
        log.info("publish skipped: PIPELINE_SERVING_URL is not set")
        return None

    con = duckdb.connect()
    # The extension is pre-installed in the Docker images; locally INSTALL downloads it once.
    con.execute("INSTALL postgres; LOAD postgres;")
    # ATTACH takes no bound parameters, so paths and URLs are inlined (quotes escaped).
    con.execute(f"ATTACH '{_escape(str(config.warehouse_path.resolve()))}' AS wh (READ_ONLY)")
    con.execute(f"ATTACH '{_escape(config.serving_url)}' AS pg (TYPE postgres)")

    present = {
        r[0]
        for r in con.execute(
            "SELECT table_name FROM duckdb_tables() WHERE database_name = 'wh'"
        ).fetchall()
    }
    rows: dict[str, int] = {}
    con.execute("BEGIN")
    try:
        for table in SERVING_TABLES:
            if table not in present:  # e.g. quality_results before the first GX run
                continue
            con.execute(f"DROP TABLE IF EXISTS pg.public.{table}")
            con.execute(f"CREATE TABLE pg.public.{table} AS SELECT * FROM wh.main.{table}")
            rows[table] = con.execute(f"SELECT count(*) FROM wh.main.{table}").fetchone()[0]
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()
    log.info("published %d tables to Postgres (%s rows)", len(rows), f"{sum(rows.values()):,}")
    return rows


def _escape(value: str) -> str:
    return value.replace("'", "''")
