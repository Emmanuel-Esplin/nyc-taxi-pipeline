"""Model: build the warehouse by running the dbt project in dbt/ for one month.

dbt replaces the hand-rolled SQL runner from Phase 1. `dbt build` runs seeds, models and
data tests in dependency order; fact_trips and the aggregates are incremental models using
delete+insert on year_month, so rerunning a month replaces exactly that month.

dbt runs as a subprocess rather than through dbtRunner: dbt-duckdb keeps the database file
open in-process after a run, which blocks the pipeline's own connections. A subprocess
releases the file when it exits, and it is how an orchestrator would call dbt anyway.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from pathlib import Path

from pipeline.config import Config
from pipeline.months import Month

log = logging.getLogger(__name__)


class DbtBuildError(RuntimeError):
    """dbt build failed: a model errored or a data test failed."""


def dbt_executable() -> str:
    """The dbt installed alongside this Python, so the venv/image's dbt is always used."""
    return str(Path(sys.executable).with_name("dbt"))


def dbt_build(config: Config, month: Month) -> None:
    """Run `dbt build` for one month. Raises DbtBuildError naming every failed node."""
    project = config.dbt_dir.resolve()
    # Keep dbt's artifacts and logs with the data, so the repo (and Docker's read-only
    # /app) stays clean and each data directory has its own manifest.
    target = config.data_dir.resolve() / "dbt" / "target"
    args = [
        dbt_executable(), "build",
        "--project-dir", str(project),
        "--profiles-dir", str(project),
        "--target-path", str(target),
        "--log-path", str(target.parent / "logs"),
        "--vars", json.dumps({"year_month": month.key}),
    ]  # fmt: skip
    # profiles.yml and sources.yml read these, so dbt sees the same paths as the pipeline.
    env = {
        **os.environ,
        "PIPELINE_DATA_DIR": str(config.data_dir.resolve()),
        "PIPELINE_WAREHOUSE_PATH": str(config.warehouse_path.resolve()),
    }
    proc = subprocess.run(args, env=env, capture_output=True, text=True, check=False)

    if proc.returncode != 0:
        log.error("dbt output:\n%s", proc.stdout[-4000:] + proc.stderr[-2000:])
        failed = _failed_nodes(target / "run_results.json")
        raise DbtBuildError(f"dbt build for {month} failed: {', '.join(failed) or 'see logs'}")
    log.info("dbt build %s: %s", month, _summary_line(proc.stdout))


def _failed_nodes(run_results: Path) -> list[str]:
    """Names of nodes that errored or failed, from dbt's run_results.json artifact."""
    if not run_results.exists():  # dbt failed before running anything (e.g. parse error)
        return []
    results = json.loads(run_results.read_text())["results"]
    return [r["unique_id"].split(".")[-1] for r in results if r["status"] in {"error", "fail"}]


def _summary_line(stdout: str) -> str:
    """dbt's closing 'Done. PASS=.. WARN=.. ERROR=..' line, for the pipeline log."""
    done = [line for line in stdout.splitlines() if "Done. PASS=" in line]
    return done[-1].split("Done. ", 1)[-1].strip() if done else "completed"
