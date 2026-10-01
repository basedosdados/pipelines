"""Bootstrap: clean the cx LABSTAT flat files into the series and annual tables.

The transform lives in ``pipelines.datasets.us_bls_cex.utils`` so this one-shot
load and the recurring Prefect pipeline share one implementation.

Usage:
    python models/us_bls_cex/code/clean_labstat.py [--tables series annual]
"""

import argparse
import logging
import sys
import time
from pathlib import Path

# The shared venv's editable install may point at another checkout; import the
# pipelines package from this repo.
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.us_bls_cex.pumd_files import LABSTAT_DIR, OUTPUT_DIR
from pipelines.datasets.us_bls_cex.utils import clean_labstat

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tables", nargs="*", choices=["series", "annual"])
    args = ap.parse_args()
    t0 = time.time()
    result = clean_labstat(LABSTAT_DIR, OUTPUT_DIR, args.tables)
    logging.info(f"done in {time.time() - t0:.0f}s: {result}")


if __name__ == "__main__":
    main()
