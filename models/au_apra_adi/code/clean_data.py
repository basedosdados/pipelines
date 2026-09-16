#!/usr/bin/env python3
"""Bootstrap: clean the APRA ADI performance workbook into partitioned all-STRING
parquet. Transform lives in pipelines.datasets.au_apra_adi.utils (shared with the
recurring pipeline). Data lives outside the repo:
    ~/Downloads/au_apra_adi_data/{input,output}

Usage:
    uv run python models/au_apra_adi/code/clean_data.py
"""

import logging
import os
from pathlib import Path

from pipelines.datasets.au_apra_adi.utils import clean_all

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

DATA = Path(
    os.environ.get(
        "AU_APRA_ADI_DATA", Path.home() / "Downloads" / "au_apra_adi_data"
    )
)

if __name__ == "__main__":
    result = clean_all(DATA / "input", DATA / "output")
    print("max_year_quarter:", result["max_year_quarter"])
