"""One-shot bootstrap: clean an NPPES monthly bundle into partitioned parquet.

Thin CLI over the shared transform in ``pipelines/datasets/us_hhs_nppes/utils.py``
so the bootstrap and the recurring pipeline never drift.

Env vars:
    NPPES_INPUT_DIR   default ~/Downloads/us_hhs_nppes_data/input
    NPPES_OUTPUT_DIR  default ~/Downloads/us_hhs_nppes_data/output
    NPPES_DOWNLOAD    if set, download the current monthly ZIP first
"""

import os
import time
from pathlib import Path

from pipelines.datasets.us_hhs_nppes.utils import clean_all, download_monthly

HOME = Path.home()
INPUT_DIR = Path(
    os.environ.get(
        "NPPES_INPUT_DIR", HOME / "Downloads/us_hhs_nppes_data/input"
    )
)
OUTPUT_DIR = Path(
    os.environ.get(
        "NPPES_OUTPUT_DIR", HOME / "Downloads/us_hhs_nppes_data/output"
    )
)


def main():
    if os.environ.get("NPPES_DOWNLOAD"):
        print("downloading monthly bundle ...", flush=True)
        print(" ->", download_monthly(INPUT_DIR))
    print(f"Input:  {INPUT_DIR}")
    print(f"Output: {OUTPUT_DIR}")
    t0 = time.time()
    # write_header=True: the one-shot onboarding upload needs the 0-row
    # 00_header.parquet guard for table-approve. The recurring pipeline must
    # NOT have it — see PartitionWriter.
    result = clean_all(INPUT_DIR, OUTPUT_DIR, write_header=True)
    print("=== DONE ===")
    print(f"elapsed: {time.time() - t0:,.0f}s")
    print("extraction_date:", result["extraction_date"])
    for table, n in result["counts"].items():
        print(f"  {table:<20} {n:>12,}")


if __name__ == "__main__":
    main()
