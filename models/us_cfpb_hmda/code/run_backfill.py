"""Backfill all HMDA years one at a time: download -> clean -> delete the raw input.

  uv run --with duckdb python run_backfill.py            # all years, both eras
  uv run --with duckdb python run_backfill.py modern     # modern only
  uv run --with duckdb python run_backfill.py legacy 2011 2012

Skips a year whose output parquet already exists and is non-empty. Deletes the input CSV
only after its clean succeeds, so peak disk stays near a single raw file (~5 GB).
"""

import sys

from clean import clean
from common import LEGACY, LEGACY_YEARS, MODERN, MODERN_YEARS, OUTPUT
from download import download


def done(table: str, year: int) -> bool:
    p = OUTPUT / table / f"year={year}" / "data.parquet"
    return p.exists() and p.stat().st_size > 0


def run(era: str, table: str, years) -> None:
    for y in years:
        if done(table, y):
            print(f"[skip] {table} {y} already cleaned", flush=True)
            continue
        print(f"[{era} {y}] downloading...", flush=True)
        src = download(era, y)
        print(f"[{era} {y}] cleaning...", flush=True)
        clean(table, y)
        try:
            src.unlink()
            print(f"[{era} {y}] removed raw input", flush=True)
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    yrs = [int(a) for a in sys.argv[2:]] or None
    if which in ("all", "modern"):
        run("modern", MODERN, yrs or MODERN_YEARS)
    if which in ("all", "legacy"):
        run("legacy", LEGACY, yrs or LEGACY_YEARS)
    print("backfill complete.", flush=True)
