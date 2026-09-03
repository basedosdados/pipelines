"""One-shot download + clean for us_state_foreign_assistance.

Thin wrapper over the shared transform in
``pipelines/datasets/us_state_foreign_assistance/utils.py`` (the recurring
Prefect flow imports the same functions). Scratch data lives OUTSIDE the repo,
default ``~/Downloads/us_state_foreign_assistance_data`` (override with
US_STATE_FOREIGN_ASSISTANCE_DATA):

    input/   raw CSVs from the ForeignAssistance.gov S3 bucket
    output/  <table>/<table>_<year>.parquet (+ 00_header.parquet), all-STRING

Run from the repo root:  uv run python models/us_state_foreign_assistance/code/clean.py
"""

import argparse
import os
from pathlib import Path

from pipelines.datasets.us_state_foreign_assistance.constants import constants
from pipelines.datasets.us_state_foreign_assistance.utils import (
    clean_all,
    download_all,
)

DATA_DIR = Path(
    os.environ.get(
        "US_STATE_FOREIGN_ASSISTANCE_DATA",
        str(constants.DEFAULT_DATA_DIR.value),
    )
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--memory-limit", default="12GB")
    parser.add_argument("--threads", type=int, default=4)
    args = parser.parse_args()

    input_dir = DATA_DIR / "input"
    output_dir = DATA_DIR / "output"
    if not args.skip_download:
        for table, path in download_all(input_dir).items():
            print(
                f"{table}: {path.name} {path.stat().st_size:,} B", flush=True
            )
    counts = clean_all(
        input_dir,
        output_dir,
        memory_limit=args.memory_limit,
        threads=args.threads,
    )
    for table, n in counts.items():
        print(f"{table}: {n:,} rows", flush=True)


if __name__ == "__main__":
    main()
