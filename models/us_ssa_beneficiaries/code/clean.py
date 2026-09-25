"""One-shot onboarding build for us_ssa_beneficiaries.

Imports the cleaning transform from ``pipelines.datasets.us_ssa_beneficiaries.utils``
so the recurring Prefect pipeline and this bootstrap can never diverge.

    uv run python models/us_ssa_beneficiaries/code/clean.py [--download]

Raw downloads and cleaned parquet go to ``~/Downloads/us_ssa_beneficiaries_data``
(override with ``SSA_DATA_DIR``), never inside the repo or Dropbox.
"""

# ruff: noqa: E402  (sys.path must be set before the pipelines import)
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from pipelines.datasets.us_ssa_beneficiaries import utils as U  # noqa: N812
from pipelines.datasets.us_ssa_beneficiaries.constants import constants

DATA_DIR = Path(
    os.environ.get(
        "SSA_DATA_DIR", Path.home() / "Downloads" / "us_ssa_beneficiaries_data"
    )
)

log = logging.getLogger("ssa.clean")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--download",
        action="store_true",
        help="re-fetch the source files before cleaning",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    input_dir = DATA_DIR / "input"
    output_dir = DATA_DIR / "output"
    if args.download or not any(input_dir.glob("*.json")):
        U.download_all(input_dir)

    tables = U.clean_all(input_dir)

    total = 0
    for table, df in tables.items():
        written = U.write_partitioned(
            df, table, output_dir, constants.ARCHITECTURE_DIR.value
        )
        total += written
        years = (
            f"{int(df.year.min())}-{int(df.year.max())}"
            if "year" in df.columns
            else "n/a"
        )
        log.info("%-24s %9s rows  years %s", table, f"{written:,}", years)
    log.info("total %s rows -> %s", f"{total:,}", output_dir)


if __name__ == "__main__":
    main()
