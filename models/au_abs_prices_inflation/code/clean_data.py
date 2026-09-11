#!/usr/bin/env python3
"""Bootstrap: clean the downloaded ABS workbooks into partitioned parquet.

Reads ``<data root>/input`` and writes ``<data root>/output/<table>``. The
cleaning transforms live in ``pipelines.datasets.au_abs_prices_inflation`` so
the one-shot bootstrap and the recurring Prefect pipeline share one
implementation; this CLI is just the initial-load entry point.

Usage:
    uv run python models/au_abs_prices_inflation/code/clean_data.py [table ...]

With no arguments it builds all seven tables.
"""

import logging
import os
import sys
from pathlib import Path

from pipelines.datasets.au_abs_prices_inflation.constants import constants
from pipelines.datasets.au_abs_prices_inflation.cpi import (
    clean_frequency,
)
from pipelines.datasets.au_abs_prices_inflation.cpi import (
    write_partitioned as write_cpi,
)
from pipelines.datasets.au_abs_prices_inflation.releases import (
    build_release,
    write_partitioned,
)

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
        Path.home() / "Downloads" / "au_abs_prices_inflation_data",
    )
)

TABLE_ID = constants.TABLE_ID.value
RELEASES = list(constants.RELEASES.value)


def main():
    want = set(sys.argv[1:])
    inp, out = DATA_ROOT / "input", DATA_ROOT / "output"

    for frequency, table in TABLE_ID.items():
        if want and table not in want:
            continue
        df = clean_frequency(frequency, str(inp / "cpi"))
        rows = write_cpi(df, table, str(out))
        log.info(
            "%s: %d rows | years %d-%d | %d regions | %d items",
            table,
            rows,
            df["year"].min(),
            df["year"].max(),
            df["region"].nunique(),
            df["index_name"].nunique(),
        )

    for release in RELEASES:
        if want and release not in want:
            continue
        df = build_release(release, str(inp))
        rows = write_partitioned(df, release, str(out))
        log.info(
            "%s: %d rows | years %d-%d | %d series",
            release,
            rows,
            df["year"].min(),
            df["year"].max(),
            df["series_id"].nunique(),
        )


if __name__ == "__main__":
    main()
