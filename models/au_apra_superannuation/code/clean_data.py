#!/usr/bin/env python3
"""Bootstrap: clean the APRA superannuation workbook in the input dir into
partitioned all-STRING parquet in the output dir.

The transform lives in ``pipelines.datasets.au_apra_superannuation.utils`` so the
bootstrap and the recurring Prefect pipeline share one implementation.

Data lives outside the repo (never under Dropbox) by house convention:
    ~/Downloads/au_apra_superannuation_data/{input,output}

Usage:
    uv run python models/au_apra_superannuation/code/clean_data.py
"""

import logging
import os
from pathlib import Path

from pipelines.datasets.au_apra_superannuation.utils import clean_all

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

DATA = Path(
    os.environ.get(
        "AU_APRA_SUPER_DATA",
        Path.home() / "Downloads" / "au_apra_superannuation_data",
    )
)

if __name__ == "__main__":
    result = clean_all(DATA / "input", DATA / "output")
    print("max_year_quarter:", result["max_year_quarter"])
