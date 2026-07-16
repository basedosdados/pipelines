#!/usr/bin/env python3
"""Bootstrap: clean the CPI flat files already in ../input into partitioned
parquet in ../output.

The cleaning transform lives in `pipelines.datasets.us_bls_cpi.utils` so the
one-shot bootstrap and the recurring Prefect pipeline share one implementation.
This CLI is just the initial-load entry point.

Usage:
    uv run python models/us_bls_cpi/code/clean_data.py [table ...]
"""

import logging
import sys
from pathlib import Path

from pipelines.datasets.us_bls_cpi.utils import (
    build_long,
    build_table,
    write_partitioned,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

ROOT = Path(__file__).resolve().parents[1]


def main():
    want = set(sys.argv[1:]) or {"monthly", "annual", "semiannual"}
    df = build_long(ROOT / "input")
    for tbl in ("monthly", "annual", "semiannual"):
        if tbl in want:
            write_partitioned(build_table(df, tbl), tbl, ROOT / "output")


if __name__ == "__main__":
    main()
