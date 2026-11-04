"""Load pipeline settings from config.yaml with environment-variable overrides."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

DEFAULT_CONFIG = Path("config.yaml")

ENV_OVERRIDES = {
    "PIPELINE_DATA_DIR": ("paths", "data_dir"),
    "PIPELINE_WAREHOUSE_PATH": ("paths", "warehouse_path"),
    "PIPELINE_DBT_DIR": ("paths", "dbt_dir"),
    "PIPELINE_SOURCE_DIR": ("source", "source_dir"),
}


@dataclass(frozen=True)
class ValidationConfig:
    max_trip_hours: float = 24
    max_trip_distance_miles: float = 200
    max_fare_amount: float = 1000
    max_total_amount: float = 1500


@dataclass(frozen=True)
class Config:
    data_dir: Path
    warehouse_path: Path
    dbt_dir: Path
    trip_url_template: str
    zone_lookup_url: str
    source_dir: Path | None
    timeout_seconds: int
    validation: ValidationConfig

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def staging_dir(self) -> Path:
        return self.data_dir / "staging"


def load_config(path: Path | str | None = None) -> Config:
    """Read config.yaml (if present), apply PIPELINE_* env overrides and fill defaults.

    Env vars win over the file so the same image can run locally, in CI and in Docker with
    only the environment changing.
    """
    path = Path(path) if path else DEFAULT_CONFIG
    raw = yaml.safe_load(path.read_text()) if path.exists() else {}
    raw.setdefault("paths", {})
    raw.setdefault("source", {})

    for env_var, (section, key) in ENV_OVERRIDES.items():
        if value := os.environ.get(env_var):
            raw[section][key] = value

    paths, source = raw["paths"], raw["source"]
    source_dir = source.get("source_dir")
    return Config(
        data_dir=Path(paths.get("data_dir", "data")),
        warehouse_path=Path(paths.get("warehouse_path", "data/warehouse.duckdb")),
        dbt_dir=Path(paths.get("dbt_dir", "dbt")),
        trip_url_template=source["trip_url_template"],
        zone_lookup_url=source["zone_lookup_url"],
        source_dir=Path(source_dir) if source_dir else None,
        timeout_seconds=int(source.get("timeout_seconds", 120)),
        validation=ValidationConfig(**raw.get("validation", {})),
    )
