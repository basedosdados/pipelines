#!/usr/bin/env python3
"""Bootstrap: download the BLS flat files and clean them into partitioned parquet.

The download and cleaning transform live in
``pipelines.datasets.us_bls_employment.utils`` so the one-shot bootstrap and the
recurring Prefect pipeline share one implementation. This CLI is only the
initial-load entry point.

Raw downloads and cleaned parquet stay outside the repo and outside Dropbox, at
``~/Downloads/us_bls_employment_data`` by default (override with
``US_BLS_EMPLOYMENT_DATA``), so neither a multi-gigabyte sync nor an accidental
commit of data is possible.

Usage:
    uv run python models/us_bls_employment/code/clean_data.py [--download] [table ...]
"""

import json
import logging
import os
import sys
from pathlib import Path

from pipelines.datasets.us_bls_employment.constants import constants
from pipelines.datasets.us_bls_employment.utils import (
    build_dicionario,
    build_program,
    download_flatfiles,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

DATA_ROOT = Path(
    os.environ.get(
        "US_BLS_EMPLOYMENT_DATA",
        os.path.expanduser("~/Downloads/us_bls_employment_data"),
    )
)


def main() -> None:
    """Rebuild the requested tables from ``input/`` into ``output/``.

    Table slugs may be passed as argv; with none, all four data tables plus the
    dictionary are rebuilt. ``--download`` fetches the flat files first.
    """
    argv = sys.argv[1:]
    download = "--download" in argv
    want = [a for a in argv if not a.startswith("--")]
    tables = want or constants.DATA_TABLES.value
    inp, out = DATA_ROOT / "input", DATA_ROOT / "output"
    if download:
        download_flatfiles(inp)
    summary = {}
    for table in tables:
        summary[table] = build_program(inp, out, out / "_shards", table)
    if not want or "dicionario" in want:
        build_dicionario(inp, out)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
