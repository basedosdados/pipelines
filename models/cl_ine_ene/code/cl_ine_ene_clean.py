#!/usr/bin/env python3
"""One-shot download and clean of the full cl_ine_ene back-series.

The transform itself lives in ``pipelines/datasets/cl_ine_ene/utils.py`` and is
imported, not duplicated: the recurring Prefect flow runs the same code, so a fix
in one place is a fix in both.

Scratch data goes under ~/Downloads/cl_ine_ene_data (never inside the repo or
Dropbox), overridable with CL_INE_ENE_DATA.

    python models/cl_ine_ene/code/cl_ine_ene_clean.py --download --clean
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys

REPO = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO))

from pipelines.datasets.cl_ine_ene import utils  # noqa: E402

DATA = pathlib.Path(
    os.environ.get(
        "CL_INE_ENE_DATA", pathlib.Path.home() / "Downloads/cl_ine_ene_data"
    )
)
INPUT, OUTPUT = DATA / "input", DATA / "output"
TABLE = "microdato"


def resolve_last(argument: str | None) -> tuple[int, int]:
    if argument:
        year, month = argument.split("-")
        return int(year), int(month)
    # Walk forward from a period known to exist rather than guessing the lag.
    return utils.source_max_period((2026, 1))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--download", action="store_true")
    parser.add_argument("--clean", action="store_true")
    parser.add_argument(
        "--last",
        help="last moving quarter, YYYY-MM (default: probe the source)",
    )
    parser.add_argument("--first", default="2010-02")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="skip periods whose parquet already exists",
    )
    args = parser.parse_args()

    first = tuple(int(p) for p in args.first.split("-"))
    last = resolve_last(args.last)
    wanted = utils.periods(first, last)
    print(
        f"{len(wanted)} periods, {wanted[0][0]}-{wanted[0][1]:02d} .. {last[0]}-{last[1]:02d}"
    )

    if args.download:
        INPUT.mkdir(parents=True, exist_ok=True)
        for index, (year, month) in enumerate(wanted, 1):
            utils.download_period(year, month, INPUT)
            if index % 20 == 0 or index == len(wanted):
                print(f"  downloaded {index}/{len(wanted)}", flush=True)

    if args.clean:
        counts = utils.clean_all(
            INPUT, OUTPUT / TABLE, wanted, skip_existing=args.resume
        )
        total = sum(counts.values())
        print(f"\ncleaned {len(counts)} periods, {total:,} rows")
        (DATA / "row_counts.json").write_text(json.dumps(counts, indent=1))
        missing = [
            f"{y}-{m:02d}" for y, m in wanted if f"{y}-{m:02d}" not in counts
        ]
        if missing:
            raise SystemExit(f"periods missing from the output: {missing}")


if __name__ == "__main__":
    main()
