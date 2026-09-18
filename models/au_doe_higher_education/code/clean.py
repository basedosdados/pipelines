"""One-shot onboarding build for au_doe_higher_education.

Reads the downloaded source workbooks and writes one partitioned Parquet
dataset per table. The transform and the build itself live in
``pipelines/datasets/au_doe_higher_education/utils.py`` so the recurring flow
and this bootstrap cannot drift apart; this file only supplies the scratch
paths and prints a summary.

Scratch data lives outside the repo and outside Dropbox:
``~/Downloads/au_doe_higher_education_data/{input,output}``.

The bootstrap differs from a scheduled run in one way that matters: it reads
every pivot vintage present in ``input/`` (``enrol_v2020.xlsx`` ...
``enrol_v2024.xlsx``), which is what reaches back to 2016. A scheduled run can
only download the current release, because the department delists the older
ones.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.au_doe_higher_education.utils import (
    build_all,
    build_institution_directory,
    collect_provider_codes,
    observed_institutions,
    write_partitioned,
)

DATA = Path(
    os.environ.get(
        "AU_DOE_DATA",
        Path.home() / "Downloads" / "au_dese_higher_education_data",
    )
)
INPUT = DATA / "input"
OUTPUT = DATA / "output"

PARTITION_OVERRIDE = {"student_completion_rate": "cohort_start_year"}


def main() -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)

    built = build_all(INPUT)
    for table, frame in built.items():
        print(f"{table:36} {len(frame):>8,} rows")

    observed = observed_institutions(built)

    codes: dict[str, str] = {}
    codes.update(
        collect_provider_codes(
            INPUT / "sec15_attrition.xlsx",
            [f"15.{n}" for n in range(1, 10)],
            1,
        )
    )
    codes.update(
        collect_provider_codes(
            INPUT / "sec16_equityperf.xlsx",
            [f"16.{n}" for n in range(1, 14)],
            1,
        )
    )
    directory = build_institution_directory(
        INPUT / "inst_list_2020.xls", observed, codes
    )

    missing = set(observed["institution_id"]) - set(
        directory["id_higher_education_institution"]
    )
    if missing:
        raise SystemExit(
            f"institutions missing from the directory: {sorted(missing)}"
        )

    for table, frame in built.items():
        rows = write_partitioned(
            frame, OUTPUT, table, PARTITION_OVERRIDE.get(table, "year")
        )
        print(f"wrote {table:36} {rows:>8,} rows")

    directory.to_parquet(OUTPUT / "higher_education_institution.parquet")
    print(
        f"wrote {'higher_education_institution':36} {len(directory):>8,} rows"
    )
    print(
        f"provider codes resolved: {directory['provider_code'].notna().sum()}"
    )


if __name__ == "__main__":
    main()
