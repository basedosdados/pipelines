"""One-shot bootstrap: download and clean the FHFA HPI files for us_fhfa_hpi.

The transform itself lives in ``pipelines/datasets/us_fhfa_hpi/utils.py`` and is
shared with the recurring Prefect pipeline — this script only wires it to a local
scratch directory so the dataset can be built once and uploaded to dev.

Scratch data never goes in the repo or under Dropbox. Default root is
``~/Downloads/us_fhfa_hpi_data`` (override with ``FHFA_HPI_DATA_DIR``).

Run:
    uv run python models/us_fhfa_hpi/code/clean_data.py            # clean only
    uv run python models/us_fhfa_hpi/code/clean_data.py --download # fetch first
"""

import argparse
import logging
import os
from pathlib import Path

from pipelines.datasets.us_fhfa_hpi.utils import (
    clean_all,
    download_all,
)

DATA_DIR = Path(
    os.environ.get(
        "FHFA_HPI_DATA_DIR", Path.home() / "Downloads" / "us_fhfa_hpi_data"
    )
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--download", action="store_true", help="fetch the source files first"
    )
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    input_dir = args.data_dir / "input"
    output_dir = args.data_dir / "output"

    if args.download:
        download_all(input_dir)

    result = clean_all(input_dir, output_dir)
    counts = result["counts"]
    total = sum(counts.values())
    print()
    for table, n in counts.items():
        print(f"{table:22s} {n:>10,}")
    print(f"{'TOTAL':22s} {total:>10,}")
    print(f"\nmaster latest month: {result['max_year_month']}")
    print(f"annual latest year:  {result['max_year']}")


if __name__ == "__main__":
    main()
