"""Validate: tag every row with the rules it fails, then split clean from quarantined.

Nothing is silently dropped. Rejected rows are written to staging/rejected/ with the list
of failed rules so they can be inspected, fixed upstream, or re-admitted later.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from pipeline.months import Month
from pipeline.rules import NATURAL_KEY, Rule

log = logging.getLogger(__name__)

TAGGED_TABLE = "tagged_trips"


def rejected_path(staging_dir: Path, month: Month) -> Path:
    partition = staging_dir / "rejected" / f"year={month.year}" / f"month={month.month:02d}"
    return partition / "rejected.parquet"


@dataclass
class ValidationResult:
    rows_in: int
    rows_clean: int
    rows_rejected: int
    failures_by_rule: dict[str, int] = field(default_factory=dict)


def tag_sql(source: str, rules: list[Rule]) -> str:
    """SELECT over `source` adding failed_rules: the names of every rule the row fails."""
    checks = ",\n        ".join(
        f"CASE WHEN coalesce(({rule.failure_sql}), TRUE) THEN '{rule.name}' END" for rule in rules
    )
    return f"""
    WITH ranked AS (
        SELECT *,
               row_number() OVER (PARTITION BY {", ".join(NATURAL_KEY)} ORDER BY source_row)
                   AS _dup_rank
        FROM {source}
    )
    SELECT * EXCLUDE (_dup_rank),
           [name FOR name IN [
        {checks}
           ] IF name IS NOT NULL] AS failed_rules
    FROM ranked
    """


def apply_rules(con: duckdb.DuckDBPyConnection, source: str, rules: list[Rule]) -> None:
    """Materialise the tagged rows as a temp table. `zone_lookup` must already exist."""
    con.execute(f"CREATE OR REPLACE TEMP TABLE {TAGGED_TABLE} AS {tag_sql(source, rules)}")


def summarise(con: duckdb.DuckDBPyConnection, rules: list[Rule]) -> ValidationResult:
    rows_in, rows_rejected = con.execute(
        f"SELECT count(*), count(*) FILTER (WHERE len(failed_rules) > 0) FROM {TAGGED_TABLE}"
    ).fetchone()
    counts = dict(
        con.execute(
            f"SELECT rule, count(*) FROM (SELECT unnest(failed_rules) AS rule FROM {TAGGED_TABLE}) "
            "GROUP BY rule"
        ).fetchall()
    )
    return ValidationResult(
        rows_in=rows_in,
        rows_clean=rows_in - rows_rejected,
        rows_rejected=rows_rejected,
        failures_by_rule={rule.name: counts.get(rule.name, 0) for rule in rules},
    )


def write_rejected(con: duckdb.DuckDBPyConnection, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.part")
    con.execute(
        f"""
        COPY (
            SELECT *, array_to_string(failed_rules, ', ') AS reject_reason
            FROM {TAGGED_TABLE}
            WHERE len(failed_rules) > 0
            ORDER BY source_row
        ) TO '{tmp}' (FORMAT parquet, COMPRESSION zstd)
        """
    )
    tmp.replace(path)


def validate(
    con: duckdb.DuckDBPyConnection, source: str, rules: list[Rule], rejected_path: Path
) -> ValidationResult:
    apply_rules(con, source, rules)
    result = summarise(con, rules)
    write_rejected(con, rejected_path)
    log.info(
        "validated %s rows: %s clean, %s rejected (%.2f%%)",
        f"{result.rows_in:,}",
        f"{result.rows_clean:,}",
        f"{result.rows_rejected:,}",
        100 * result.rows_rejected / max(result.rows_in, 1),
    )
    for name, count in result.failures_by_rule.items():
        if count:
            log.info("  rule %-26s %10s rows", name, f"{count:,}")
    return result
