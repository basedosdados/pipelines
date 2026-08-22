"""One-shot bootstrap: clean every fiscal year of the USAspending archive.

Imports the transform from ``pipelines.datasets.us_treasury_usaspending.utils``
rather than duplicating it, so the recurring pipeline and this bootstrap can
never drift apart.

Each archive is downloaded (or picked up if the prefetcher already got it),
cleaned into all-STRING partitioned parquet, and then deleted — the zips total
~50 GB and the machine does not need to hold them all.

Usage:
    uv run python models/us_treasury_usaspending/code/bootstrap_clean.py
    uv run python models/us_treasury_usaspending/code/bootstrap_clean.py --years 2007-2010
    uv run python models/us_treasury_usaspending/code/bootstrap_clean.py --keep-archives
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

from pipelines.datasets.us_treasury_usaspending.constants import constants
from pipelines.datasets.us_treasury_usaspending.utils import (
    archive_name,
    clean_archive,
    download_archive,
    write_dicionario,
)

DATA_DIR = Path(
    os.environ.get(
        "USASPENDING_DATA_DIR",
        Path.home() / "Downloads" / "us_treasury_usaspending_data",
    )
)
INPUT = DATA_DIR / "input"
OUTPUT = DATA_DIR / "output"
COUNTS_FILE = DATA_DIR / "row_counts.json"


def parse_years(spec: str | None) -> list[int]:
    if not spec:
        return list(range(constants.FIRST_FISCAL_YEAR.value, 2027))
    if "-" in spec:
        lo, hi = spec.split("-")
        return list(range(int(lo), int(hi) + 1))
    return [int(x) for x in spec.split(",")]


def load_counts() -> dict:
    return json.loads(COUNTS_FILE.read_text()) if COUNTS_FILE.exists() else {}


def save_counts(counts: dict) -> None:
    COUNTS_FILE.write_text(json.dumps(counts, indent=1, sort_keys=True))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", help="e.g. 2007-2010 or 2019,2020")
    ap.add_argument(
        "--stamp", default="20260806", help="archive publication stamp"
    )
    ap.add_argument("--keep-archives", action="store_true")
    ap.add_argument("--skip-dicionario", action="store_true")
    args = ap.parse_args()

    counts = load_counts()
    for fy in parse_years(args.years):
        for family, table in constants.AWARD_FAMILIES.value.items():
            key = f"{table}/{fy}"
            if key in counts:
                print(f"skip {key} ({counts[key]:,} rows already cleaned)")
                continue
            name = archive_name(fy, family, args.stamp)
            t0 = time.time()
            print(f"{time.strftime('%H:%M:%S')} {name} …", flush=True)
            zip_path = download_archive(fy, family, args.stamp, INPUT)
            got = clean_archive(
                zip_path, table, OUTPUT, expected_fiscal_year=fy
            )
            for got_fy, n in got.items():
                counts[f"{table}/{got_fy}"] = (
                    counts.get(f"{table}/{got_fy}", 0) + n
                )
            save_counts(counts)
            if not args.keep_archives:
                zip_path.unlink(missing_ok=True)
                Path(str(zip_path) + ".done").unlink(missing_ok=True)
            total = sum(got.values())
            print(
                f"  {total:,} rows in {time.time() - t0:.0f}s"
                f"{'' if args.keep_archives else ' (archive removed)'}",
                flush=True,
            )

    if not args.skip_dicionario:
        rows_csv = DATA_DIR / "ref" / "dicionario_data.csv"
        if rows_csv.exists():
            n = write_dicionario(rows_csv, OUTPUT)
            counts["dicionario"] = n
            save_counts(counts)
            print(f"dicionario: {n:,} rows")
        else:
            print(
                f"dicionario skipped: {rows_csv} not found (run build_architecture.py)"
            )

    print("\nrow counts:")
    for key in sorted(counts):
        print(f"  {key:<40s} {counts[key]:>12,}")
    print(f"  {'TOTAL':<40s} {sum(counts.values()):>12,}")


if __name__ == "__main__":
    main()
