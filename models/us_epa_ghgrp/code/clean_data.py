"""One-shot bootstrap: download and clean the GHGRP API tables for us_epa_ghgrp.

The transform itself lives in ``pipelines/datasets/us_epa_ghgrp/utils.py`` and is
shared with the recurring Prefect pipeline — this script only wires it to a local
scratch directory so the dataset can be built once and uploaded to dev.

Scratch data never goes in the repo or under Dropbox. Default root is
``~/Downloads/us_epa_ghgrp_data`` (override with ``EPA_GHGRP_DATA_DIR``).

Run:
    uv run python models/us_epa_ghgrp/code/clean_data.py            # clean only
    uv run python models/us_epa_ghgrp/code/clean_data.py --download # fetch first
"""

import argparse
import logging
import os
from pathlib import Path

from pipelines.datasets.us_epa_ghgrp.utils import clean_all, download_all

DATA_DIR = Path(
    os.environ.get(
        "EPA_GHGRP_DATA_DIR", Path.home() / "Downloads" / "us_epa_ghgrp_data"
    )
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--download", action="store_true", help="fetch the API tables first"
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
    for table, n in result["counts"].items():
        print(f"{table:20s} {n:>10,} rows")
    print(f"max year: {result['max_year']}")


if __name__ == "__main__":
    main()
