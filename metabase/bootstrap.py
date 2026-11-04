"""Build the Metabase dashboard from code, through Metabase's REST API.

    python metabase/bootstrap.py            # or: make bi-bootstrap

Idempotent: the first run completes Metabase's setup (admin user), connects the `analytics`
Postgres database through the read-only role, and creates the collection, questions and
dashboard. Later runs log in and update the same objects in place, so this file is the
dashboard's source of truth. It finishes by enabling a public link to the dashboard, which
the README screenshot is taken from.

Standard library only, so it runs from the host with any Python 3.11+.

Environment (all optional; defaults match docker-compose.yml for local use):
    MB_URL                    http://localhost:3000
    MB_ADMIN_EMAIL            admin@nyc-taxi.local
    MB_ADMIN_PASSWORD         local-dev-Taxi2025
    METABASE_READER_PASSWORD  local-dev-reader
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path

MB_URL = os.environ.get("MB_URL", "http://localhost:3000").rstrip("/")
ADMIN_EMAIL = os.environ.get("MB_ADMIN_EMAIL", "admin@nyc-taxi.local")
ADMIN_PASSWORD = os.environ.get("MB_ADMIN_PASSWORD", "local-dev-Taxi2025")
READER_PASSWORD = os.environ.get("METABASE_READER_PASSWORD", "local-dev-reader")

DATABASE_NAME = "NYC Taxi analytics"
COLLECTION_NAME = "NYC Taxi"
DASHBOARD_NAME = "NYC Yellow Taxi: demand, revenue and data quality"
PUBLIC_URL_FILE = Path(__file__).resolve().parents[1] / "data" / "metabase_public_url.txt"

# Latest successful run per month: reruns append to run_log, dashboards show current state.
LATEST_RUNS = """latest as (
    select distinct on (year_month) run_id, year_month
    from run_log where status = 'success'
    order by year_month, started_at desc
)"""


def scalar(name: str, sql: str, prefix: str = "", decimals: int | None = None) -> dict:
    column = {"prefix": prefix} if prefix else {}
    if decimals is not None:
        column["decimals"] = decimals
    settings = {"column_settings": {json.dumps(["name", name]): column}} if column else {}
    return {"name": name, "display": "scalar", "sql": sql, "settings": settings}


def chart(name: str, display: str, sql: str, dimensions: list[str], metrics: list[str], **extra):
    settings = {"graph.dimensions": dimensions, "graph.metrics": metrics, **extra}
    return {"name": name, "display": display, "sql": sql, "settings": settings}


def table(name: str, sql: str) -> dict:
    return {"name": name, "display": "table", "sql": sql, "settings": {}}


# Questions, keyed by a stable slug. Column aliases are what the charts display.
# Postgres's round(x, n) takes numeric only, so double-precision columns are cast first.
CARDS = {
    "trips": scalar("Clean trips", 'select sum(trips) as "Clean trips" from agg_hourly_revenue'),
    "revenue": scalar(
        "Revenue",
        'select sum(total_revenue) as "Revenue" from agg_hourly_revenue',
        prefix="$",
        decimals=0,
    ),
    "avg_total": scalar(
        "Average trip total",
        'select round(sum(total_revenue) / sum(trips), 2) as "Average trip total" '
        "from agg_hourly_revenue",
        prefix="$",
        decimals=2,
    ),
    "quarantined": scalar(
        "Rows quarantined (%)",
        f"with {LATEST_RUNS}\n"
        'select round(100.0 * sum(r.rows_rejected) / sum(r.rows_in), 1) as "Rows quarantined (%)"\n'
        "from run_log r join latest using (run_id)",
        decimals=1,
    ),
    "daily": chart(
        "Daily trips",
        "line",
        'select pickup_date as "Date", sum(trips) as "Trips"\n'
        "from agg_hourly_revenue group by 1 order by 1",
        ["Date"],
        ["Trips"],
    ),
    "hourly": chart(
        "Average trips per day, by pickup hour",
        "bar",
        'select pickup_hour as "Pickup hour",\n'
        '       round(sum(trips)::numeric / count(distinct pickup_date)) as "Avg trips per day"\n'
        "from agg_hourly_revenue group by 1 order by 1",
        ["Pickup hour"],
        ["Avg trips per day"],
    ),
    "zones": chart(
        "Top 10 pickup zones",
        "row",
        "select zone || ' (' || borough || ')' as \"Zone\", sum(trips) as \"Trips\"\n"
        "from agg_daily_zone_demand group by 1 order by 2 desc limit 10",
        ["Zone"],
        ["Trips"],
    ),
    "tipping": table(
        "Tipping by payment type",
        'select payment_description as "Payment type",\n'
        '       sum(trips) as "Trips",\n'
        "       round((sum(pct_trips_tipped * trips) / sum(trips))::numeric, 1)\n"
        '           as "Trips tipped (%)",\n'
        '       round(sum(total_tips) / sum(trips), 2) as "Average tip ($)"\n'
        "from agg_payment_tip_behaviour group by 1 order by 2 desc",
    ),
    "rejections": chart(
        "Quarantined rows by rule and month",
        "bar",
        f"with {LATEST_RUNS}\n"
        'select r.year_month as "Month", r.rule as "Rule", r.rows_failed as "Rows failed"\n'
        "from run_rejections r join latest using (run_id)\n"
        "where r.rows_failed > 0 order by 1, 3 desc",
        ["Month", "Rule"],
        ["Rows failed"],
        **{"stackable.stack_type": "stacked"},
    ),
    "gx": table(
        "Great Expectations results (latest run per month)",
        f"with {LATEST_RUNS}\n"
        'select q.year_month as "Month", q.suite as "Suite",\n'
        "       case when q.blocking then 'Contract (blocking)' else 'Drift report' end "
        'as "Type",\n'
        "       q.successful || ' / ' || q.evaluated as \"Expectations passed\",\n"
        "       case when q.success then 'Pass' else 'Fail' end as \"Result\"\n"
        "from quality_results q join latest using (run_id) order by 1, 2",
    ),
    "runs": table(
        "Pipeline run history",
        'select year_month as "Month", status as "Status", started_at as "Started (UTC)",\n'
        '       round(duration_seconds::numeric, 1) as "Seconds",\n'
        '       rows_in as "Rows in", rows_clean as "Clean", rows_rejected as "Quarantined"\n'
        "from run_log order by started_at desc limit 20",
    ),
}

# Dashboard layout on Metabase's 24-column grid: (slug or "## heading", col, row, width, height).
LAYOUT = [
    ("## Demand and revenue · Jan–Mar 2025 · TLC Yellow Taxi", 0, 0, 24, 1),
    ("trips", 0, 1, 6, 3),
    ("revenue", 6, 1, 6, 3),
    ("avg_total", 12, 1, 6, 3),
    ("quarantined", 18, 1, 6, 3),
    ("daily", 0, 4, 16, 6),
    ("hourly", 16, 4, 8, 6),
    ("zones", 0, 10, 12, 7),
    ("tipping", 12, 10, 12, 7),
    ("## Data quality · quarantine rules and Great Expectations", 0, 17, 24, 1),
    ("rejections", 0, 18, 14, 7),
    ("gx", 14, 18, 10, 7),
    ("runs", 0, 25, 24, 6),
]


class Metabase:
    def __init__(self, url: str):
        self.url = url
        self.session: str | None = None

    def call(self, method: str, path: str, body: dict | None = None):
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(f"{self.url}/api{path}", data=data, method=method)
        request.add_header("Content-Type", "application/json")
        if self.session:
            request.add_header("X-Metabase-Session", self.session)
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                raw = response.read()
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"{method} {path} -> {exc.code}: {exc.read()[:500]!r}") from exc
        return json.loads(raw) if raw else None

    def wait_until_healthy(self, timeout: int = 300) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                if self.call("GET", "/health").get("status") == "ok":
                    return
            except (RuntimeError, OSError):
                pass
            time.sleep(5)
        raise TimeoutError(f"Metabase at {self.url} not healthy after {timeout}s")

    def login(self) -> None:
        """Complete first-time setup if needed, otherwise log in as the admin."""
        props = self.call("GET", "/session/properties")
        if not props.get("has-user-setup"):
            user = {"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD,
                    "first_name": "Pipeline", "last_name": "Admin"}  # fmt: skip
            prefs = {"site_name": "NYC Taxi Pipeline", "allow_tracking": False}
            body = {"token": props["setup-token"], "user": user, "prefs": prefs}
            self.session = self.call("POST", "/setup", body)["id"]
            print(f"completed Metabase setup; admin is {ADMIN_EMAIL}")
        else:
            body = {"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD}
            self.session = self.call("POST", "/session", body)["id"]


def ensure_database(mb: Metabase) -> int:
    details = {"host": "postgres", "port": 5432, "dbname": "analytics",
               "user": "metabase_reader", "password": READER_PASSWORD, "ssl": False}  # fmt: skip
    existing = next(
        (d for d in mb.call("GET", "/database")["data"] if d["name"] == DATABASE_NAME), None
    )
    if existing:
        db_id = existing["id"]
        mb.call("PUT", f"/database/{db_id}", {"details": details})
    else:
        db_id = mb.call("POST", "/database", {"engine": "postgres", "name": DATABASE_NAME,
                                              "details": details})["id"]  # fmt: skip
    mb.call("POST", f"/database/{db_id}/sync_schema")
    return db_id


def ensure_collection(mb: Metabase) -> int:
    existing = next(
        (c for c in mb.call("GET", "/collection") if c.get("name") == COLLECTION_NAME), None
    )
    return (
        existing["id"]
        if existing
        else mb.call("POST", "/collection", {"name": COLLECTION_NAME})["id"]
    )


def collection_items(mb: Metabase, collection_id: int, model: str) -> dict[str, int]:
    items = mb.call("GET", f"/collection/{collection_id}/items?models={model}")["data"]
    return {item["name"]: item["id"] for item in items}


def ensure_cards(mb: Metabase, db_id: int, collection_id: int) -> dict[str, int]:
    existing = collection_items(mb, collection_id, "card")
    ids = {}
    for slug, card in CARDS.items():
        body = {
            "name": card["name"],
            "collection_id": collection_id,
            "display": card["display"],
            "visualization_settings": card["settings"],
            "dataset_query": {
                "database": db_id,
                "type": "native",
                "native": {"query": card["sql"]},
            },
        }
        if card["name"] in existing:
            ids[slug] = existing[card["name"]]
            mb.call("PUT", f"/card/{ids[slug]}", body)
        else:
            ids[slug] = mb.call("POST", "/card", body)["id"]
    return ids


def ensure_dashboard(mb: Metabase, collection_id: int, card_ids: dict[str, int]) -> int:
    existing = collection_items(mb, collection_id, "dashboard")
    if DASHBOARD_NAME in existing:
        dash_id = existing[DASHBOARD_NAME]
    else:
        body = {"name": DASHBOARD_NAME, "collection_id": collection_id}
        dash_id = mb.call("POST", "/dashboard", body)["id"]

    dashcards = []
    for i, (item, col, row, width, height) in enumerate(LAYOUT, start=1):
        dashcard = {"id": -i, "col": col, "row": row, "size_x": width, "size_y": height,
                    "parameter_mappings": []}  # fmt: skip
        if item.startswith("## "):  # a text card used as a section heading
            dashcard["card_id"] = None
            dashcard["visualization_settings"] = {
                "virtual_card": {"name": None, "display": "heading", "visualization_settings": {},
                                 "dataset_query": {}, "archived": False},
                "text": item.removeprefix("## "),
            }  # fmt: skip
        else:
            dashcard["card_id"] = card_ids[item]
            dashcard["visualization_settings"] = {}
        dashcards.append(dashcard)
    # Negative ids mean "new": the whole layout is replaced on every run.
    mb.call("PUT", f"/dashboard/{dash_id}", {"dashcards": dashcards, "width": "full"})
    return dash_id


def publish_dashboard(mb: Metabase, dash_id: int) -> str:
    mb.call("PUT", "/setting/enable-public-sharing", {"value": True})
    uuid = mb.call("POST", f"/dashboard/{dash_id}/public_link")["uuid"]
    return f"{MB_URL}/public/dashboard/{uuid}"


def main() -> None:
    mb = Metabase(MB_URL)
    mb.wait_until_healthy()
    mb.login()
    db_id = ensure_database(mb)
    collection_id = ensure_collection(mb)
    card_ids = ensure_cards(mb, db_id, collection_id)
    dash_id = ensure_dashboard(mb, collection_id, card_ids)
    public_url = publish_dashboard(mb, dash_id)

    PUBLIC_URL_FILE.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_URL_FILE.write_text(public_url + "\n")
    print(f"dashboard:   {MB_URL}/dashboard/{dash_id}")
    print(f"public link: {public_url}")


if __name__ == "__main__":
    main()
