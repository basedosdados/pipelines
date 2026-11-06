#!/usr/bin/env python3
"""Bootstrap: clean the EPU/GPR source files in the scratch input dir into
partitioned parquet in the scratch output dir.

The cleaning transform lives in ``pipelines.datasets.us_epu_gpr.utils`` so the
one-shot bootstrap and the recurring Prefect pipeline share one implementation.
This CLI is just the initial-load entry point.

Scratch data lives under ``~/Downloads/us_epu_gpr_data/`` (never in the repo or
Dropbox), overridable with ``EPU_GPR_DATA_DIR``.

Usage:
    uv run --no-project python models/us_epu_gpr/code/clean_data.py [--download]
"""

import logging
import os
import sys
from pathlib import Path

from pipelines.datasets.us_epu_gpr.utils import clean_all, download_all

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

DATA = Path(
    os.environ.get(
        "EPU_GPR_DATA_DIR", Path.home() / "Downloads" / "us_epu_gpr_data"
    )
)


def main():
    """Download (optional) and rebuild all tables from ``input/`` into ``output/``."""
    inp, out = DATA / "input", DATA / "output"
    if "--download" in sys.argv[1:]:
        download_all(inp)
    result = clean_all(inp, out)
    logging.info(f"max_year_month={result['max_year_month']}")


if __name__ == "__main__":
    main()
