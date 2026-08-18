"""Download, clean and discard one program year at a time.

Peak disk stays near a single program year (about 10 GB) rather than the
roughly 105 GB the full series would occupy if downloaded up front.

    uv run --with duckdb python run_all.py            # every program year
    uv run --with duckdb python run_all.py 2019 2020  # just these
"""

import json
import sys
import time

import clean
import constants as c
import download

COUNTS_PATH = c.DATA_ROOT / "row_counts.json"


def _load() -> dict:
    if COUNTS_PATH.exists():
        with open(COUNTS_PATH) as fh:
            return json.load(fh)
    return {}


def _save(counts: dict) -> None:
    with open(COUNTS_PATH, "w") as fh:
        json.dump(counts, fh, indent=1, sort_keys=True)


def run_year(year: int, counts: dict) -> None:
    started = time.time()
    print(f"\n=== program year {year}")
    download.detail(year)
    for table, rows in clean.clean_detail(year).items():
        counts.setdefault(table, {})[str(year)] = rows
    for kind in c.DETAIL_KINDS:
        path = c.INPUT_DIR / f"{kind}_{year}.csv"
        if path.exists():
            path.unlink()
    print(f"  done in {time.time() - started:.0f}s, inputs removed")


if __name__ == "__main__":
    years = [int(a) for a in sys.argv[1:]] or c.ALL_YEARS
    counts = _load()

    for year in years:
        run_year(year, counts)
        _save(counts)

    if not sys.argv[1:]:
        print("\n=== entity tables")
        download.profiles()
        for table, rows in clean.clean_profiles().items():
            counts[table] = rows
        _save(counts)

        print("\n=== summary reports")
        download.summaries()
        for key, rows in clean.clean_summaries().items():
            table, year = key.split("/")
            counts.setdefault(table, {})[year] = rows
        _save(counts)

    print(f"\nrow counts written to {COUNTS_PATH}")
