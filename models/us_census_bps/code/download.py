"""Download the Census Building Permits Survey ASCII files.

Thin CLI over ``pipelines.datasets.us_census_bps.utils.download_all``, so the
one-shot bootstrap and the recurring pipeline share one implementation.

Writes to ``$BPS_DATA_DIR/input`` (default
``~/Downloads/us_census_bps_data/input``).
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.us_census_bps.utils import download_all

DATA_DIR = Path(
    os.environ.get(
        "BPS_DATA_DIR", Path.home() / "Downloads/us_census_bps_data"
    )
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument(
        "--through-year", type=int, default=time.gmtime().tm_year
    )
    args = parser.parse_args()
    counts = download_all(
        DATA_DIR / "input",
        through_year=args.through_year,
        workers=args.workers,
    )
    print(f"done: {counts}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
