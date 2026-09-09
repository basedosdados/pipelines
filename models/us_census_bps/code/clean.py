"""Clean the downloaded Census Building Permits Survey files into Parquet.

Reads from ``$BPS_DATA_DIR/input`` and writes ``$BPS_DATA_DIR/output``
(default ``~/Downloads/us_census_bps_data``). The transform itself lives in
``pipelines.datasets.us_census_bps.utils`` so the recurring pipeline and this
one-shot bootstrap cannot drift apart.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.us_census_bps.utils import clean_all

DATA_DIR = Path(
    os.environ.get(
        "BPS_DATA_DIR", Path.home() / "Downloads/us_census_bps_data"
    )
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("tables", nargs="*", help="restrict to these tables")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(message)s", stream=sys.stdout
    )
    totals = clean_all(
        DATA_DIR / "input",
        DATA_DIR / "output",
        only=set(args.tables) or None,
    )
    print("\n=== rows written ===")
    for table, rows in sorted(totals.items()):
        print(f"  {table:24s} {rows:>12,}")
    print(f"  {'TOTAL':24s} {sum(totals.values()):>12,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
