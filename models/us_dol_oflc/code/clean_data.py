"""Clean the OFLC disclosure files into one partitioned table per program.

A thin CLI over the shared transform in
``pipelines/datasets/us_dol_oflc/utils.py``, so the one-shot onboarding run and
the recurring Prefect pipeline apply exactly the same cleaning code.

Reads every source workbook in ``<OFLC_DATA_DIR>/input``, resolves its columns
through the committed crosswalk, coerces types, derives annualised wages, and
writes hive-partitioned all-STRING Snappy Parquet to
``<OFLC_DATA_DIR>/output/<program>/year=<FY>/data.parquet``. One fiscal year is
held in memory at a time.

Usage:
    uv run python models/us_dol_oflc/code/clean_data.py [program ...]
        [--years 2020-2026] [--resume]
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from pipelines.datasets.us_dol_oflc.constants import constants
from pipelines.datasets.us_dol_oflc.utils import build

DATA = Path(
    os.environ.get("OFLC_DATA_DIR", Path.home() / "Downloads/us_dol_oflc_data")
)
INPUT = DATA / "input"
OUTPUT = DATA / "output"
REPORT = DATA / "clean_report.json"

PROGRAMS = constants.PROGRAMS.value


def main() -> int:
    argv = sys.argv[1:]
    resume = "--resume" in argv
    argv = [a for a in argv if a != "--resume"]
    years = None
    if "--years" in argv:
        i = argv.index("--years")
        lo, _, hi = argv[i + 1].partition("-")
        years = set(range(int(lo), int(hi or lo) + 1))
        argv = argv[:i] + argv[i + 2 :]
    wanted = [a for a in argv if a in PROGRAMS] or PROGRAMS
    report_path = (
        REPORT
        if not years
        else REPORT.with_name(
            f"clean_report_{wanted[0]}_{min(years)}_{max(years)}.json"
        )
    )
    report = (
        json.loads(report_path.read_text()) if report_path.exists() else {}
    )
    for program in wanted:
        print(f"=== {program} ===", flush=True)
        build(program, report, INPUT, OUTPUT, years, resume)
        report_path.write_text(json.dumps(report, indent=1))
    for program, info in report.items():
        if info.get("unrecognised_wage_units"):
            print(
                f"WARNING {program}: unrecognised wage units "
                f"{info['unrecognised_wage_units']}",
                file=sys.stderr,
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
