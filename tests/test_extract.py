"""source_available: the readiness check the Airflow sensor polls before extracting."""

from __future__ import annotations

import urllib.error
from dataclasses import replace
from pathlib import Path

import pytest

from pipeline import extract
from pipeline.config import load_config
from pipeline.months import Month

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def config():
    return replace(load_config(ROOT / "config.yaml"), source_dir=None)


def test_local_source_is_available_only_when_the_file_exists(config):
    local = replace(config, source_dir=ROOT / "tests" / "fixtures")
    assert extract.source_available(local, Month(2025, 1))
    assert not extract.source_available(local, Month(2025, 2))


@pytest.mark.parametrize("code", [403, 404])
def test_unpublished_month_is_not_available(config, monkeypatch, code):
    def not_found(url, timeout):
        raise urllib.error.HTTPError(url, code, "missing", {}, None)

    monkeypatch.setattr(extract, "_remote_size", not_found)
    assert not extract.source_available(config, Month(2099, 1))


def test_published_month_is_available(config, monkeypatch):
    monkeypatch.setattr(extract, "_remote_size", lambda url, timeout: 60_000_000)
    assert extract.source_available(config, Month(2025, 1))


def test_server_errors_are_raised_not_treated_as_missing(config, monkeypatch):
    def server_error(url, timeout):
        raise urllib.error.HTTPError(url, 503, "unavailable", {}, None)

    monkeypatch.setattr(extract, "_remote_size", server_error)
    with pytest.raises(urllib.error.HTTPError):
        extract.source_available(config, Month(2025, 1))
