"""Model: build the star schema in warehouse.duckdb from the SQL files in sql/.

SQL files are plain SELECTs, like dbt models; this module supplies the materialisation:

* ``table``       - rebuilt every run (small dimensions)
* ``incremental`` - DELETE the month, then INSERT it (fact and aggregates)

Everything for one month - dimensions, facts, aggregates and the run_log entry - commits in a
single transaction, so a month is either fully loaded or untouched, and rerunning a month
replaces it with identical rows.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path

import duckdb

from pipeline.months import Month

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Model:
    name: str
    path: str
    materialized: str  # "table" | "incremental"


MODELS = [
    Model("dim_zone", "marts/dim_zone.sql", "table"),
    Model("dim_date", "marts/dim_date.sql", "table"),
    Model("dim_payment_type", "marts/dim_payment_type.sql", "table"),
    Model("fact_trips", "marts/fact_trips.sql", "incremental"),
    Model("agg_daily_zone_demand", "aggregates/agg_daily_zone_demand.sql", "incremental"),
    Model("agg_hourly_revenue", "aggregates/agg_hourly_revenue.sql", "incremental"),
    Model("agg_payment_tip_behaviour", "aggregates/agg_payment_tip_behaviour.sql", "incremental"),
]

_PLACEHOLDER = re.compile(r"\{\{\s*(\w+)\s*\}\}")


def render(sql: str, variables: dict[str, str]) -> str:
    """Substitute {{ name }} placeholders with SQL string literals."""

    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in variables:
            raise KeyError(f"SQL references undefined variable {{{{ {name} }}}}")
        return "'" + str(variables[name]).replace("'", "''") + "'"

    return _PLACEHOLDER.sub(replace, sql)


def build_models(
    con: duckdb.DuckDBPyConnection,
    sql_dir: Path,
    month: Month,
    staging_trips: Path,
    zone_lookup: Path,
) -> dict[str, int]:
    """Materialise every model for `month`. Caller owns the transaction."""
    variables = {
        "year_month": month.key,
        "staging_trips": str(staging_trips),
        "zone_lookup": str(zone_lookup),
    }
    rows: dict[str, int] = {}
    for model in MODELS:
        query = render((sql_dir / model.path).read_text(), variables)
        if model.materialized == "table":
            con.execute(f"CREATE OR REPLACE TABLE {model.name} AS {query}")
            rows[model.name] = con.execute(f"SELECT count(*) FROM {model.name}").fetchone()[0]
        elif model.materialized == "incremental":
            con.execute(
                f"CREATE TABLE IF NOT EXISTS {model.name} AS SELECT * FROM ({query}) WHERE false"
            )
            con.execute(f"DELETE FROM {model.name} WHERE year_month = ?", [month.key])
            con.execute(f"INSERT INTO {model.name} BY NAME {query}")
            rows[model.name] = con.execute(
                f"SELECT count(*) FROM {model.name} WHERE year_month = ?", [month.key]
            ).fetchone()[0]
        else:
            raise ValueError(f"Unknown materialization {model.materialized!r} for {model.name}")
        log.info("model %-26s %10s rows", model.name, f"{rows[model.name]:,}")
    return rows
