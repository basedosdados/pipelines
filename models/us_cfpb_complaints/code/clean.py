"""Clean the CFPB bulk export into staging parquet (one-shot onboarding entry point).

The transform lives in ``pipelines/datasets/us_cfpb_complaints/utils.py`` and is shared
with the recurring pipeline; this is only the CLI around it.

    python clean.py                  # full run over <DATA_DIR>/input
    python clean.py --limit 200000   # smoke test
"""

import argparse
from pathlib import Path

from common import COMPLAINT, CSV_NAME, INPUT, OUTPUT, clean_complaint


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--csv", type=Path, default=INPUT / CSV_NAME)
    ap.add_argument("--output", type=Path, default=OUTPUT)
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    res = clean_complaint(args.csv, args.output, args.limit)
    print(f"\n=== {COMPLAINT}: ROWS PER YEAR ===")
    for y in sorted(res["per_year"]):
        print(f"  {y}  {res['per_year'][y]:>10,}")
    print("\n=== STATS ===")
    for k, v in sorted(res["stats"].items()):
        print(f"  {k:24s} {v:>12,}")
    print(f"\nmax date_received: {res['max_date_received']}")


if __name__ == "__main__":
    main()
