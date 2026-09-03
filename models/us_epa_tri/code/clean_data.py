"""One-shot bootstrap: download and clean every TRI reporting year.

Thin CLI over the shared transform in ``pipelines/datasets/us_epa_tri/utils.py``
so the bootstrap and the recurring pipeline never drift.

Env vars:
    TRI_DATA_DIR   default ~/Downloads/us_epa_tri_data (input/, ref/, output/)
    TRI_DOWNLOAD   if set, download the national files first (slow: ~60 MB per
                   year at ~150 KB/s per connection) and the Envirofacts
                   TRI_FACILITY table
    TRI_YEARS      optional comma-separated subset of years to clean

Run from the repo root with the repo on PYTHONPATH:
    PYTHONPATH=. uv run python models/us_epa_tri/code/clean_data.py
"""

import logging
import os
import time
from pathlib import Path

from pipelines.datasets.us_epa_tri.utils import (
    clean_all,
    download_facility_fips,
    download_year,
    source_years,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

DATA_DIR = Path(
    os.environ.get("TRI_DATA_DIR", Path.home() / "Downloads/us_epa_tri_data")
)
INPUT_DIR = DATA_DIR / "input"
REF_DIR = DATA_DIR / "ref"
OUTPUT_DIR = DATA_DIR / "output"


def main():
    years = None
    if os.environ.get("TRI_YEARS"):
        years = [int(y) for y in os.environ["TRI_YEARS"].split(",")]
    if os.environ.get("TRI_DOWNLOAD"):
        for y in years or source_years():
            print(" ->", download_year(y, INPUT_DIR))
        print(" ->", download_facility_fips(REF_DIR))
    print(f"Input:  {INPUT_DIR}")
    print(f"Output: {OUTPUT_DIR}")
    t0 = time.time()
    # write_header=True: the one-shot onboarding upload needs the 0-row
    # 00_header.parquet guard for table-approve. The recurring pipeline must
    # NOT have it (dump_header would infer INT64 from an empty frame).
    result = clean_all(
        INPUT_DIR,
        OUTPUT_DIR,
        REF_DIR / "tri_facility_fips.csv",
        years=years,
        write_header=True,
    )
    print("=== DONE ===")
    print(f"elapsed: {time.time() - t0:,.0f}s")
    print("max_year:", result["max_year"])
    for table, n in result["counts"].items():
        print(f"  {table:<12} {n:>12,}")
    for year, note in sorted(result["notes"].items()):
        if note["facilities_without_fips"] or note["facility_conflicts"]:
            print(f"  {year}: {note}")


if __name__ == "__main__":
    main()
