"""Build the ~10k-row integration fixture from a full monthly TLC file.

    python scripts/make_fixture.py data/raw/yellow_tripdata_2025-01.parquet

A pure random sample would almost never contain the rare failures (22 out-of-month rows
in 3.5M), so the fixture is a seeded random sample plus every rare edge case we validate,
plus a few synthetic rows (duplicates, unknown zones) that the real month does not contain.
The output is deterministic for a given input file.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

TARGET_ROWS = 10_000
OUT = Path("tests/fixtures/yellow_tripdata_2025-01.parquet")

# Up to EDGE_ROWS_PER_CASE rows of each rare failure, so every rule fires in the fixture.
EDGE_ROWS_PER_CASE = 20
EDGE_CASES = [
    "date_trunc('month', tpep_pickup_datetime) <> DATE '2025-01-01'",
    "tpep_dropoff_datetime <= tpep_pickup_datetime",
    "tpep_dropoff_datetime - tpep_pickup_datetime >= INTERVAL 24 HOUR",
    "trip_distance > 200",
    "fare_amount > 1000",
    "total_amount > 1500",
]


def main(source: str) -> None:
    con = duckdb.connect()
    con.execute(f"CREATE TABLE src AS SELECT * FROM read_parquet('{source}')")
    edge = " UNION ".join(
        f"(SELECT * FROM src WHERE {case} ORDER BY ALL LIMIT {EDGE_ROWS_PER_CASE})"
        for case in EDGE_CASES
    )
    con.execute(f"CREATE TABLE edge AS {edge}")
    # Synthetic: 5 exact duplicates and 3 trips between zone ids not in the lookup.
    con.execute(
        "CREATE TABLE dupes AS SELECT * FROM src WHERE passenger_count = 2 ORDER BY ALL LIMIT 5"
    )
    con.execute(
        "CREATE TABLE unknown_zone AS "
        "SELECT * REPLACE (999 AS PULocationID, 998 AS DOLocationID) FROM src "
        "WHERE passenger_count = 3 ORDER BY ALL LIMIT 3"
    )
    extra = sum(
        con.execute(f"SELECT count(*) FROM {t}").fetchone()[0] * k
        for t, k in [("edge", 1), ("dupes", 2), ("unknown_zone", 1)]
    )
    sample_size = TARGET_ROWS - extra
    OUT.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"""
        COPY (
            SELECT * FROM (
                SELECT * FROM src USING SAMPLE reservoir({sample_size} ROWS) REPEATABLE (42)
                UNION ALL SELECT * FROM edge
                UNION ALL SELECT * FROM dupes
                UNION ALL SELECT * FROM dupes
                UNION ALL SELECT * FROM unknown_zone
            ) ORDER BY tpep_pickup_datetime, tpep_dropoff_datetime, PULocationID
        ) TO '{OUT}' (FORMAT parquet, COMPRESSION zstd)
        """
    )
    rows = con.execute("SELECT count(*) FROM read_parquet(?)", [str(OUT)]).fetchone()[0]
    print(f"wrote {OUT}: {rows} rows")


if __name__ == "__main__":
    main(sys.argv[1])
