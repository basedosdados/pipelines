"""One-shot bootstrap: download and clean the full us_bls_oes panel.

Imports the cleaning transform from `pipelines/datasets/us_bls_oes/utils.py` so
the bootstrap and the recurring pipeline cannot drift apart (see AGENTS.md,
"DRY with the onboarding code").

Raw downloads and cleaned parquet stay outside the repo and outside Dropbox —
default `~/Downloads/us_bls_oes_data/`, overridable with OES_INPUT_DIR and
OES_OUTPUT_DIR.

Run:
    uv run python models/us_bls_oes/code/clean_data.py            # 2003-latest
    uv run python models/us_bls_oes/code/clean_data.py 2010 2025  # a subset
"""

import logging
import os
import sys
from pathlib import Path

from pipelines.datasets.us_bls_oes.constants import constants
from pipelines.datasets.us_bls_oes.utils import (
    clean_year,
    download_release,
    latest_source_year,
)

ROOT = Path(
    os.environ.get("OES_DATA_DIR", Path.home() / "Downloads/us_bls_oes_data")
)
INPUT_DIR = Path(os.environ.get("OES_INPUT_DIR", ROOT / "input"))
OUTPUT_DIR = Path(os.environ.get("OES_OUTPUT_DIR", ROOT / "output"))


def main(years: list[int]) -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    totals: dict[str, int] = {t: 0 for t in constants.DATA_TABLES.value}
    for year in years:
        download_release(INPUT_DIR, year)
        for table, n in clean_year(INPUT_DIR, OUTPUT_DIR, year).items():
            totals[table] += n
    print("\n=== totals ===")
    for table, n in totals.items():
        print(f"  {table:9s} {n:>12,} rows")
    print(f"  years     {years[0]}-{years[-1]} ({len(years)})")
    print(f"  output    {OUTPUT_DIR}")


if __name__ == "__main__":
    if len(sys.argv) == 3:
        years = list(range(int(sys.argv[1]), int(sys.argv[2]) + 1))
    elif len(sys.argv) > 1:
        years = [int(a) for a in sys.argv[1:]]
    else:
        years = list(
            range(constants.FIRST_YEAR.value, latest_source_year() + 1)
        )
    main(years)
