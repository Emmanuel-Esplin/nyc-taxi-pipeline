"""Schema: present the raw TLC file as a typed, snake_case relation.

TLC files are not schema-stable across years (e.g. `airport_fee` vs `Airport_fee`, and
`cbd_congestion_fee` only exists from 2025). Columns are matched case-insensitively and any
that are missing become typed NULLs, so every month lands with the same staging schema.
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb

log = logging.getLogger(__name__)

# Name of the temp view that validation reads from.
TYPED_VIEW = "typed_trips"

# staging column -> (raw column, lower-cased; DuckDB type).
# Money is DECIMAL rather than DOUBLE so monthly revenue sums are exact to the cent.
COLUMNS: dict[str, tuple[str, str]] = {
    "vendor_id": ("vendorid", "INTEGER"),
    "pickup_datetime": ("tpep_pickup_datetime", "TIMESTAMP"),
    "dropoff_datetime": ("tpep_dropoff_datetime", "TIMESTAMP"),
    "passenger_count": ("passenger_count", "INTEGER"),
    "trip_distance": ("trip_distance", "DOUBLE"),
    "rate_code_id": ("ratecodeid", "INTEGER"),
    "store_and_fwd_flag": ("store_and_fwd_flag", "VARCHAR"),
    "pu_location_id": ("pulocationid", "INTEGER"),
    "do_location_id": ("dolocationid", "INTEGER"),
    "payment_type": ("payment_type", "INTEGER"),
    "fare_amount": ("fare_amount", "DECIMAL(12, 2)"),
    "extra": ("extra", "DECIMAL(12, 2)"),
    "mta_tax": ("mta_tax", "DECIMAL(12, 2)"),
    "tip_amount": ("tip_amount", "DECIMAL(12, 2)"),
    "tolls_amount": ("tolls_amount", "DECIMAL(12, 2)"),
    "improvement_surcharge": ("improvement_surcharge", "DECIMAL(12, 2)"),
    "total_amount": ("total_amount", "DECIMAL(12, 2)"),
    "congestion_surcharge": ("congestion_surcharge", "DECIMAL(12, 2)"),
    "airport_fee": ("airport_fee", "DECIMAL(12, 2)"),
    "cbd_congestion_fee": ("cbd_congestion_fee", "DECIMAL(12, 2)"),
}


def load_zone_lookup(con: duckdb.DuckDBPyConnection, zones_csv: Path) -> None:
    """Load the zone lookup as the `zone_lookup` temp table used by the zone rules."""
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE zone_lookup AS
        SELECT CAST(LocationID AS INTEGER) AS location_id,
               Borough AS borough, Zone AS zone, service_zone
        FROM read_csv('{zones_csv}', header = true)
        """
    )


def create_typed_view(con: duckdb.DuckDBPyConnection, raw_path: Path) -> None:
    """Rename to snake_case and TRY_CAST, so unparseable values become NULL and fail rules."""
    # file_row_number gives every row a stable id (source_row) that is carried into the
    # rejected files, so a quarantined row can be traced back to its position in the raw file.
    source = f"read_parquet('{raw_path}', file_row_number = true)"
    described = con.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()
    present = {row[0].lower(): row[0] for row in described}
    selects = ["file_row_number AS source_row"]
    for target, (raw_name, sql_type) in COLUMNS.items():
        if raw_name in present:
            selects.append(f'TRY_CAST("{present[raw_name]}" AS {sql_type}) AS {target}')
        else:
            log.debug("raw file has no %s column; filling with NULL", raw_name)
            selects.append(f"CAST(NULL AS {sql_type}) AS {target}")
    columns = ",\n    ".join(selects)
    con.execute(f"CREATE OR REPLACE TEMP VIEW {TYPED_VIEW} AS SELECT\n    {columns}\nFROM {source}")
