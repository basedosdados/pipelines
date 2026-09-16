#!/usr/bin/env python3
"""Bootstrap: download the BIS selected property prices flat CSV and clean it
into partitioned parquet.

The cleaning transform lives in
``pipelines.datasets.world_bis_property_prices.utils`` so the one-shot bootstrap
and the recurring Prefect pipeline share one implementation. This CLI is just
the initial-load entry point.

Scratch data lives under ``~/Downloads/world_bis_property_prices_data/``
(``input/`` and ``output/``) — never in the repo or Dropbox. Override with the
``WORLD_BIS_PP_DATA`` environment variable.

Usage:
    uv run python models/world_bis_property_prices/code/clean_data.py [--download]
"""

import logging
import os
import sys
from pathlib import Path

from pipelines.datasets.world_bis_property_prices.utils import (
    clean_all,
    download_flatfile,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

DATA_DIR = Path(
    os.environ.get(
        "WORLD_BIS_PP_DATA",
        Path.home() / "Downloads" / "world_bis_property_prices_data",
    )
)


def main():
    """Clean ``input/`` into ``output/``; pass ``--download`` to fetch first."""
    input_dir = DATA_DIR / "input"
    output_dir = DATA_DIR / "output"
    if (
        "--download" in sys.argv[1:]
        or not (input_dir / "WS_SPP_csv_flat.csv").exists()
    ):
        download_flatfile(input_dir)
    result = clean_all(input_dir, output_dir)
    print(f"max_year_quarter: {result['max_year_quarter']}")
    print(f"output: {result['price_index']}")


if __name__ == "__main__":
    main()
