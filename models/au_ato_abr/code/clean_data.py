"""One-shot bootstrap: clean the ABN Bulk Extract into partitioned parquet.

Thin CLI over the shared transform in ``pipelines/datasets/au_ato_abr/utils.py``
so the bootstrap and the recurring pipeline never drift. Output is all-STRING
hive-partitioned parquet, partitioned by ``extraction_date`` (see utils for why).

Env vars:
    ABR_INPUT_DIR   default ~/Downloads/au_ato_abr_data/input   (holds the ZIPs)
    ABR_OUTPUT_DIR  default ~/Downloads/au_ato_abr_data/output
    ABR_DOWNLOAD    if set, (re)download the source ZIPs into ABR_INPUT_DIR first
"""

import os
from pathlib import Path

from pipelines.datasets.au_ato_abr.utils import clean_all, download_zips

HOME = Path.home()
INPUT_DIR = Path(
    os.environ.get("ABR_INPUT_DIR", HOME / "Downloads/au_ato_abr_data/input")
)
OUTPUT_DIR = Path(
    os.environ.get("ABR_OUTPUT_DIR", HOME / "Downloads/au_ato_abr_data/output")
)


def main():
    if os.environ.get("ABR_DOWNLOAD"):
        download_zips(INPUT_DIR)
    print(f"Input:  {INPUT_DIR}")
    print(f"Output: {OUTPUT_DIR}")
    result = clean_all(INPUT_DIR, OUTPUT_DIR)
    print("=== DONE ===")
    print("counts:", result["counts"])
    print("max_extraction_date:", result["max_extraction_date"])


if __name__ == "__main__":
    main()
