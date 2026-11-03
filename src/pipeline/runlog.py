"""Audit trail: one run_log row per pipeline run, plus per-rule rejection counts.

run_log is append-only on purpose. Reruns replace a month's data but add a new log row, so
the history of every attempt (including failures) survives.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

import duckdb

DDL = """
CREATE TABLE IF NOT EXISTS run_log (
    run_id            VARCHAR PRIMARY KEY,
    year_month        VARCHAR NOT NULL,
    status            VARCHAR NOT NULL,      -- success | failed
    started_at        TIMESTAMP NOT NULL,
    finished_at       TIMESTAMP NOT NULL,
    duration_seconds  DOUBLE NOT NULL,
    raw_file          VARCHAR,
    raw_bytes         BIGINT,
    rows_in           BIGINT,
    rows_clean        BIGINT,
    rows_rejected     BIGINT,
    error             VARCHAR
);
CREATE TABLE IF NOT EXISTS run_rejections (
    run_id       VARCHAR NOT NULL,
    year_month   VARCHAR NOT NULL,
    rule         VARCHAR NOT NULL,
    rows_failed  BIGINT NOT NULL
);
"""


@dataclass
class RunRecord:
    """One pipeline attempt. Filled in stage by stage, then written once at the end."""

    run_id: str
    year_month: str
    started_at: datetime
    status: str = "running"
    finished_at: datetime | None = None
    raw_file: str | None = None
    raw_bytes: int | None = None
    rows_in: int | None = None
    rows_clean: int | None = None
    rows_rejected: int | None = None
    error: str | None = None
    failures_by_rule: dict[str, int] = field(default_factory=dict)

    @property
    def duration_seconds(self) -> float:
        end = self.finished_at or datetime.now()
        return round((end - self.started_at).total_seconds(), 3)


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Create the audit tables on first use (idempotent)."""
    con.execute(DDL)


def record_run(con: duckdb.DuckDBPyConnection, run: RunRecord) -> None:
    """Append the run and its per-rule counts. Runs inside the caller's transaction, if any."""
    ensure_tables(con)
    con.execute(
        "INSERT INTO run_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            run.run_id,
            run.year_month,
            run.status,
            run.started_at,
            run.finished_at,
            run.duration_seconds,
            run.raw_file,
            run.raw_bytes,
            run.rows_in,
            run.rows_clean,
            run.rows_rejected,
            run.error,
        ],
    )
    if run.failures_by_rule:
        con.executemany(
            "INSERT INTO run_rejections VALUES (?, ?, ?, ?)",
            [[run.run_id, run.year_month, r, n] for r, n in run.failures_by_rule.items()],
        )
