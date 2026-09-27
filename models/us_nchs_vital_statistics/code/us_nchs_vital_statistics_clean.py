"""One-shot bootstrap: clean every NCHS year into partitioned parquet.

Imports the transform from ``pipelines.datasets.us_nchs_vital_statistics.utils``
so the onboarding load and the recurring pipeline share one implementation.

    python models/us_nchs_vital_statistics/code/us_nchs_vital_statistics_clean.py
    python .../us_nchs_vital_statistics_clean.py --products birth --years 2023 2024
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.us_nchs_vital_statistics import (
    utils as u,
)
from pipelines.datasets.us_nchs_vital_statistics.constants import (
    constants,
)

DATA = Path(
    os.environ.get(
        "NCHS_DATA_DIR",
        os.path.expanduser("~/Downloads/us_nchs_vital_statistics_data"),
    )
)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--products", nargs="*", default=["birth", "death"])
    ap.add_argument("--years", nargs="*", type=int)
    ap.add_argument("--skip-dicionario", action="store_true")
    ap.add_argument(
        "--skip-existing",
        action="store_true",
        help="leave already-written year partitions alone (resume a partial run)",
    )
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    t0 = time.time()
    counts = u.clean_all(
        DATA / "input",
        DATA / "output",
        products=tuple(args.products),
        years=args.years,
        skip_existing=args.skip_existing,
    )
    if not args.skip_dicionario:
        n = u.write_dicionario(constants.CODE_DIR.value, DATA / "output")
        print(f"dicionario: {n} rows")

    total = sum(counts.values())
    for product in sorted({p for p, _ in counts}):
        sub = {y: n for (p, y), n in counts.items() if p == product}
        print(f"{product}: {len(sub)} years, {sum(sub.values()):,} rows")
    print(f"TOTAL {total:,} rows in {(time.time() - t0) / 60:.1f} min")


if __name__ == "__main__":
    main()
