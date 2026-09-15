#!/usr/bin/env python3
"""Bootstrap for us_treasury_fiscaldata: download the FiscalData API and clean it
into partitioned parquet.

The download + cleaning transform lives in
``pipelines.datasets.us_treasury_fiscaldata.utils`` so this one-shot bootstrap
and the recurring Prefect pipeline share one implementation. This CLI is just
the initial-load entry point.

Scratch data lives outside the repo and outside Dropbox
(``$US_TREASURY_DATA`` or ``~/Downloads/us_treasury_fiscaldata_data``).

Usage:
    uv run python models/us_treasury_fiscaldata/code/clean_data.py [--skip-download] [table ...]
"""

import logging
import os
import sys
from pathlib import Path

from pipelines.datasets.us_treasury_fiscaldata.constants import constants
from pipelines.datasets.us_treasury_fiscaldata.utils import (
    clean_table,
    download_table,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

DATA = Path(
    os.environ.get(
        "US_TREASURY_DATA",
        str(Path.home() / "Downloads" / "us_treasury_fiscaldata_data"),
    )
)


def main():
    args = [a for a in sys.argv[1:] if a != "--skip-download"]
    skip = "--skip-download" in sys.argv
    want = set(args) or set(constants.DATA_TABLES.value)
    input_dir, output_dir = DATA / "input", DATA / "output"

    for table in constants.DATA_TABLES.value:
        if table not in want:
            continue
        if not skip:
            download_table(table, input_dir)
        res = clean_table(table, input_dir, output_dir)
        print(f"{table}: max_date={res['max_date']} -> {res['path']}")


if __name__ == "__main__":
    main()
