import pytest

from pipeline.months import Month, month_range


def test_parse_and_bounds():
    month = Month.parse("2025-12")
    assert (month.key, str(month.start), str(month.end)) == ("2025-12", "2025-12-01", "2026-01-01")


@pytest.mark.parametrize("bad", ["2025-1", "2025-13", "25-01", "2025/01", ""])
def test_parse_rejects_malformed(bad):
    with pytest.raises(ValueError):
        Month.parse(bad)


def test_month_range_spans_year_boundary():
    months = month_range(Month(2024, 11), Month(2025, 2))
    assert [m.key for m in months] == ["2024-11", "2024-12", "2025-01", "2025-02"]


def test_month_range_rejects_reversed():
    with pytest.raises(ValueError):
        month_range(Month(2025, 2), Month(2025, 1))
