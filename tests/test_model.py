import pytest

from pipeline.model import render


def test_render_quotes_values_as_sql_literals():
    sql = render("SELECT * FROM t WHERE ym = {{ year_month }}", {"year_month": "2025-01"})
    assert sql == "SELECT * FROM t WHERE ym = '2025-01'"


def test_render_escapes_single_quotes():
    assert render("{{ p }}", {"p": "o'brien"}) == "'o''brien'"


def test_render_fails_on_undefined_variable():
    with pytest.raises(KeyError):
        render("{{ missing }}", {})
