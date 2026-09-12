"""
us_fed_fred — download + clean (one-shot onboarding CLI).

Thin wrapper over the shared transform in
``pipelines/datasets/us_fed_fred/utils.py`` — the SAME code the recurring daily
pipeline runs, so the bootstrap and the pipeline cannot drift. This file only
wires the scratch data location and a CLI; the fetch, the license gate, and the
parquet writers all live in ``utils.py``.

Pulls the curated public-domain seed series (see ../SEED_SERIES.md) from the FRED
REST API and writes two all-STRING partitioned parquet outputs conforming to the
architecture CSVs:

  input/<series_id>.json                      raw fetched series (kept only)
  output/observation/year=YYYY/data.parquet   long: year, date, series_id, value
  output/series/data.parquet                  catalog: one row per series

License gate (both applied at download, in utils.download_all):
  1. Source allowlist  — keep only U.S.-federal-agency sources (public domain).
  2. "Copyright" in /series notes — FRED's own marker for restricted series.
Every dropped series is logged to input/_excluded.csv.

Credential: FRED_API_KEY from the environment ONLY (or a gitignored .env at the
scratch dir). Never hard-coded, never committed.
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from pipelines.datasets.us_fed_fred.utils import clean_all, download_all

DATA_ROOT = Path(
    os.environ.get(
        "US_FED_FRED_DATA", Path.home() / "Downloads" / "us_fed_fred_data"
    )
)


def run(limit: int | None = None) -> None:
    """Download the seed series, apply the license gate, and write parquet.

    Args:
        limit: If given, only process the first ``limit`` seed series (smoke test).
    """
    input_dir = DATA_ROOT / "input"
    output_dir = DATA_ROOT / "output"
    download_all(input_dir, limit=limit)
    result = clean_all(input_dir, output_dir)

    print("\n=== SUMMARY ===")
    print(f"series kept   : {result['n_series']}")
    print(f"observations  : {result['n_observation']:,}")
    print(f"max date      : {result['max_date']}")
    print(f"excluded log  : {input_dir / '_excluded.csv'}")
    print(f"output        : {output_dir}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--limit",
        type=int,
        default=None,
        help="only process the first N seed series (smoke test)",
    )
    args = ap.parse_args()
    run(limit=args.limit)
