#!/usr/bin/env python3
"""Bootstrap: clean the ABS CPI xlsx already in ../input into partitioned
parquet in ../output (tables ``cpi_quarterly`` and ``cpi_monthly``).

The cleaning transform lives in `pipelines.datasets.au_abs_prices_inflation.cpi` so the
one-shot bootstrap and the recurring Prefect pipeline share one implementation.
This CLI is just the initial-load entry point.

Usage:
    uv run python models/au_abs_prices_inflation/code/clean_data.py [quarterly monthly]
"""

import logging
import sys
from pathlib import Path

from pipelines.datasets.au_abs_prices_inflation.constants import constants
from pipelines.datasets.au_abs_prices_inflation.cpi import (
    clean_frequency,
    write_partitioned,
)

TABLE_ID = constants.TABLE_ID.value

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("au_abs_prices_inflation")
ROOT = Path(__file__).resolve().parents[1]


def main():
    want = set(sys.argv[1:]) or {"quarterly", "monthly"}
    for tbl in ("quarterly", "monthly"):
        if tbl not in want:
            continue
        df = clean_frequency(tbl, str(ROOT / "input"))
        n = write_partitioned(df, TABLE_ID[tbl], str(ROOT / "output"))
        log.info(
            "%s: %d rows | years %d-%d | %d regions | %d items",
            TABLE_ID[tbl],
            n,
            df["year"].min(),
            df["year"].max(),
            df["region"].nunique(),
            df["index_name"].nunique(),
        )


if __name__ == "__main__":
    main()
