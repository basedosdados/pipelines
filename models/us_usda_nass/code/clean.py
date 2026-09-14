"""One-shot onboarding bootstrap: download + clean us_usda_nass into parquet.

The download + cleaning transform lives in
``pipelines.datasets.us_usda_nass.utils`` and is imported here rather than
duplicated, so this bootstrap and the recurring flow cannot diverge (DRY).

Scratch data lives under ``~/Downloads/us_usda_nass_data`` (never Dropbox/repo):
``input/`` for the downloaded ``.gz`` sector files, ``output/`` for the
partitioned all-STRING parquet. Override with ``US_USDA_NASS_DATA``.

Usage:
    uv run python models/us_usda_nass/code/clean.py [--skip-download]

``--skip-download`` reuses the ``.gz`` files already in ``input/`` (the sector
files are large; do not re-fetch when iterating).
"""

import argparse
import os
from pathlib import Path

from pipelines.datasets.us_usda_nass.utils import clean_all, download_bulk

DATA_ROOT = Path(
    os.environ.get(
        "US_USDA_NASS_DATA",
        os.path.expanduser("~/Downloads/us_usda_nass_data"),
    )
)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--skip-download",
        action="store_true",
        help="reuse the .gz files already in input/",
    )
    args = ap.parse_args()

    input_dir = DATA_ROOT / "input"
    output_dir = DATA_ROOT / "output"

    if not args.skip_download:
        print("=== downloading bulk sector files ===")
        download_bulk(input_dir)

    print("=== cleaning ===")
    result = clean_all(input_dir, output_dir)

    print("\n=== SUMMARY ===")
    for table, count in result["counts"].items():
        print(f"  {table}: {count:,} rows -> {result[table]}")
    print(f"  dicionario -> {result['dicionario']}")
    print(f"  max_year: {result['max_year']}")


if __name__ == "__main__":
    main()
