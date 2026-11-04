"""Extract: land the month's trip file and the zone lookup in data/raw/, unchanged.

Files are written to a temp name and renamed, so a crashed download never leaves a
half-written file that a later run would mistake for a complete one.
"""

from __future__ import annotations

import logging
import shutil
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path

import duckdb

from pipeline.config import Config
from pipeline.months import Month

log = logging.getLogger(__name__)

ZONE_LOOKUP_FILE = "taxi_zone_lookup.csv"


@dataclass(frozen=True)
class ExtractResult:
    """What landed in raw/ for one month; `downloaded` is False when the file was reused."""

    trips_path: Path
    zones_path: Path
    trips_rows: int
    trips_bytes: int
    downloaded: bool


def trip_filename(month: Month) -> str:
    """TLC's own file name, kept unchanged so raw/ mirrors the source exactly."""
    return f"yellow_tripdata_{month.key}.parquet"


def extract(config: Config, month: Month) -> ExtractResult:
    """Land the month's trip file and the zone lookup in raw/, from source_dir or the CDN."""
    config.raw_dir.mkdir(parents=True, exist_ok=True)

    trips_path = config.raw_dir / trip_filename(month)
    zones_path = config.raw_dir / ZONE_LOOKUP_FILE
    if config.source_dir:
        downloaded = _copy_if_changed(config.source_dir / trip_filename(month), trips_path)
        _copy_if_changed(config.source_dir / ZONE_LOOKUP_FILE, zones_path)
    else:
        url = config.trip_url_template.format(year_month=month.key)
        downloaded = _download_if_changed(url, trips_path, config.timeout_seconds)
        _download_if_changed(config.zone_lookup_url, zones_path, config.timeout_seconds)

    rows = parquet_row_count(trips_path)
    size = trips_path.stat().st_size
    log.info("raw %s: %s rows, %.1f MB", trips_path.name, f"{rows:,}", size / 1e6)
    return ExtractResult(trips_path, zones_path, rows, size, downloaded)


def source_available(config: Config, month: Month) -> bool:
    """True once the month's file has been published (or exists in source_dir).

    TLC publishes with a lag of about two months, so a scheduler polls this before extracting.
    """
    if config.source_dir:
        return (config.source_dir / trip_filename(month)).exists()
    url = config.trip_url_template.format(year_month=month.key)
    try:
        return _remote_size(url, config.timeout_seconds) is not None
    except urllib.error.HTTPError as exc:
        if exc.code in {403, 404}:  # the CDN answers 403 for files that do not exist yet
            return False
        raise


def parquet_row_count(path: Path) -> int:
    """Row count from parquet footer metadata - no data is scanned."""
    query = "SELECT sum(num_rows) FROM parquet_file_metadata(?)"
    return int(duckdb.execute(query, [str(path)]).fetchone()[0])


def _copy_if_changed(source: Path, target: Path) -> bool:
    """Copy a local source file into raw/ unless a same-sized copy is already there."""
    if not source.exists():
        raise FileNotFoundError(f"Source file not found: {source}")
    if target.exists() and target.stat().st_size == source.stat().st_size:
        log.info("skip %s: already in raw/ with matching size", target.name)
        return False
    tmp = target.with_suffix(target.suffix + ".part")
    shutil.copyfile(source, tmp)
    tmp.replace(target)
    log.info("copied %s from %s", target.name, source.parent)
    return True


def _download_if_changed(url: str, target: Path, timeout: int) -> bool:
    """Download into raw/ unless the local file already matches the remote size."""
    # Size is a cheap change signal: TLC republishes corrected files with new sizes.
    remote_size = _remote_size(url, timeout)
    if target.exists() and remote_size is not None and target.stat().st_size == remote_size:
        log.info("skip %s: already in raw/ with matching size", target.name)
        return False
    tmp = target.with_suffix(target.suffix + ".part")
    log.info("downloading %s", url)
    with urllib.request.urlopen(url, timeout=timeout) as response, tmp.open("wb") as out:
        shutil.copyfileobj(response, out, length=1024 * 1024)  # stream in 1 MB chunks
    # A dropped connection can end the stream early without raising, so verify the size.
    if remote_size is not None and tmp.stat().st_size != remote_size:
        tmp.unlink()
        raise OSError(f"Incomplete download of {url}: expected {remote_size} bytes")
    tmp.replace(target)
    return True


def _remote_size(url: str, timeout: int) -> int | None:
    """Content-Length from a HEAD request, or None if the server does not send it."""
    request = urllib.request.Request(url, method="HEAD")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        length = response.headers.get("Content-Length")
    return int(length) if length else None
