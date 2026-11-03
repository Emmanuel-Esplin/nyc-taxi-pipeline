"""Command-line entry point: python -m pipeline run --month 2025-01."""

from __future__ import annotations

import argparse
import logging

from pipeline.config import load_config
from pipeline.months import Month, month_range
from pipeline.pipeline import run_month


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pipeline", description=__doc__)
    parser.add_argument("--config", default=None, help="Path to config.yaml")
    parser.add_argument("-v", "--verbose", action="store_true", help="Debug logging")
    commands = parser.add_subparsers(dest="command", required=True)

    run = commands.add_parser("run", help="Run the pipeline for one month or a range")
    which = run.add_mutually_exclusive_group(required=True)
    which.add_argument("--month", type=Month.parse, help="Single month, e.g. 2025-01")
    which.add_argument("--start", type=Month.parse, help="First month of a backfill")
    run.add_argument("--end", type=Month.parse, help="Last month of a backfill (inclusive)")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    config = load_config(args.config)

    if args.command == "run":
        if args.start and not args.end:
            parser.error("--start requires --end")
        months = [args.month] if args.month else month_range(args.start, args.end)
        for month in months:
            run_month(config, month)
    return 0
