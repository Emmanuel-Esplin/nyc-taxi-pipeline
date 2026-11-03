"""Helpers for the YYYY-MM month keys that partition every layer of the pipeline."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date

_PATTERN = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])$")


@dataclass(frozen=True, order=True)
class Month:
    """A calendar month, the unit every layer is partitioned and reloaded by."""

    year: int
    month: int

    @classmethod
    def parse(cls, value: str) -> Month:
        """Parse 'YYYY-MM'; also used as the argparse type for --month/--start/--end."""
        match = _PATTERN.match(value)
        if not match:
            raise ValueError(f"Expected a month like 2025-01, got {value!r}")
        return cls(int(match.group(1)), int(match.group(2)))

    @property
    def key(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"

    @property
    def start(self) -> date:
        return date(self.year, self.month, 1)

    @property
    def end(self) -> date:
        """First day of the following month (exclusive upper bound)."""
        return self.next().start

    def next(self) -> Month:
        # month % 12 + 1 wraps December to January; month // 12 bumps the year only then.
        return Month(self.year + self.month // 12, self.month % 12 + 1)

    def __str__(self) -> str:
        return self.key


def month_range(start: Month, end: Month) -> list[Month]:
    """Every month from start to end, inclusive."""
    if end < start:
        raise ValueError(f"--end {end} is before --start {start}")
    months, current = [], start
    while current <= end:
        months.append(current)
        current = current.next()
    return months
