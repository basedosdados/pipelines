#!/usr/bin/env python3
"""Bootstrap: clean the ABS sources already in ../input into partitioned parquet
in ../output.

The cleaning transform lives in `pipelines.datasets.au_abs_labour_force.utils` so
the one-shot bootstrap and the recurring Prefect pipeline share one
implementation. This CLI is just the initial-load entry point; it assumes the
SDMX CSVs (LF.csv, LF_AGES.csv, LF_UNDER.csv) and the Excel spreadsheets
(62020018.xlsx, 62020019.xlsx, SEM1.xlsx) are already in ../input.

Usage:
    uv run python models/au_abs_labour_force/code/clean_data.py [table ...]
"""

import logging
import sys
from pathlib import Path

from pipelines.datasets.au_abs_labour_force.constants import constants
from pipelines.datasets.au_abs_labour_force.utils import (
    clean_all,
    write_partitioned,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

ROOT = Path(__file__).resolve().parents[1]


def main():
    """Rebuild the requested tables from ``input/`` into ``output/``."""
    want = set(sys.argv[1:]) or set(constants.DATA_TABLES.value)
    tables = clean_all(ROOT / "input")
    for name, df in tables.items():
        if name in want:
            write_partitioned(df, name, ROOT / "output")


if __name__ == "__main__":
    main()
