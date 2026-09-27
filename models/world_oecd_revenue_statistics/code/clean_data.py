#!/usr/bin/env python3
"""Bootstrap: clean the OECD Revenue Statistics CSVs already in ``input/`` into
partitioned parquet in ``output/``.

The transform lives in ``pipelines.datasets.world_oecd_revenue_statistics.utils``
so the bootstrap and the recurring Prefect pipeline share one implementation.

Usage:
    uv run python models/world_oecd_revenue_statistics/code/clean_data.py
"""

import logging
import os
from pathlib import Path

from pipelines.datasets.world_oecd_revenue_statistics.utils import clean_all

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

DATA_DIR = Path(
    os.environ.get(
        "OECD_REV_DATA_DIR",
        Path.home() / "Downloads" / "world_oecd_revenue_statistics_data",
    )
)
STRUCTURE_CACHE = DATA_DIR / "input" / "structure" / "dsd.xml"


def main():
    result = clean_all(
        input_dir=DATA_DIR / "input",
        output_dir=DATA_DIR / "output",
        structure_cache=STRUCTURE_CACHE,
    )
    print(
        f"revenue rows: {result['rows']:,}  max_year: {result['max_year']}\n"
        f"  {result['revenue']}\n  {result['dicionario']}"
    )


if __name__ == "__main__":
    main()
