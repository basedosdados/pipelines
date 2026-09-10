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
import os
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
# Scratch data (raw downloads, cleaned parquet) never lives in the repo: the
# checkout sits inside Dropbox, so writing multi-GB output here would trigger a
# sync and risk committing data. Default to ~/Downloads and allow an override.
DATA_ROOT = Path(
    os.environ.get(
        "AU_ABS_PRICES_INFLATION_DATA",
        Path.home() / "Downloads" / "au_abs_prices_inflation_data" / "cpi",
    )
)


def main():
    want = set(sys.argv[1:]) or {"quarterly", "monthly"}
    for tbl in ("quarterly", "monthly"):
        if tbl not in want:
            continue
        df = clean_frequency(tbl, str(DATA_ROOT / "input"))
        n = write_partitioned(df, TABLE_ID[tbl], str(DATA_ROOT / "output"))
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
