"""Download and clean the SSA baby names archives into partitioned all-STRING parquet.

Usage:
    python models/us_ssa_baby_names/code/clean_data.py [--skip-download]

Writes ``$US_SSA_BABY_NAMES_DATA_DIR/{input,output}`` (default
``~/Library/Caches/us_ssa_baby_names_data``). The transform itself lives in
``pipelines/datasets/us_ssa_baby_names/utils.py``, to be shared with a recurring
pipeline.
"""

import logging
import sys

from pipelines.datasets.us_ssa_baby_names.constants import constants
from pipelines.datasets.us_ssa_baby_names.utils import clean_all, download_all

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")


def main():
    root = constants.DATA_DIR.value
    if "--skip-download" not in sys.argv:
        download_all(root / "input")
    result = clean_all(root / "input", root / "output")
    for table, n in result["rows"].items():
        print(f"{table}: {n:,} rows")
    print(f"latest year: {result['max_year']}")


if __name__ == "__main__":
    main()
