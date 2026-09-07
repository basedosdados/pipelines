#!/usr/bin/env python3
"""Report columns empty in the latest year but populated historically.

`not_null_proportion_multiple_columns` is scoped to the most recent year, so a
discontinued series looks exactly like a column that was never populated. This
tells them apart across the whole panel in one pass, instead of one test round
per column, and is how `IGNORE_IN_PROPORTION` in build_dbt_files.py was
derived.

Any column reported under "empty across the WHOLE panel" is a real defect --
usually a `safe_cast` that silently produced NULLs -- and must not be added to
the ignore list.

Usage:
    uv run python models/us_ed_nces_ccd/code/check_discontinued.py
"""

from __future__ import annotations

import json
from pathlib import Path

from google.cloud import bigquery

ROOT = Path(__file__).resolve().parent
PROJECT = "basedosdados-dev"

#: The last year each table carries, so "latest" is unambiguous.
LATEST_YEAR = {
    "school": 2024,
    "school_district": 2024,
    "school_enrollment": 2024,
    "staff": 2024,
    "district_finance": 2020,
}


def main() -> None:
    client = bigquery.Client(project=PROJECT)
    columns = json.loads((ROOT / "columns.json").read_text())

    for table, latest in LATEST_YEAR.items():
        names = [c["name"] for c in columns[table] if c["name"] != "year"]
        select = ", ".join(
            f"countif({c} is not null and year = {latest}) as cur_{i}, "
            f"countif({c} is not null) as all_{i}, "
            f"max(if({c} is not null, year, null)) as last_{i}"
            for i, c in enumerate(names)
        )
        query = f"select {select} from `{PROJECT}.us_ed_nces_ccd.{table}`"
        row = dict(next(iter(client.query(query).result())).items())

        discontinued = [
            (c, row[f"all_{i}"], row[f"last_{i}"])
            for i, c in enumerate(names)
            if row[f"cur_{i}"] == 0 and row[f"all_{i}"] > 0
        ]
        never = [c for i, c in enumerate(names) if row[f"all_{i}"] == 0]

        print(f"\n{table} (latest year {latest})")
        print(
            f"  discontinued, empty in {latest} but populated before: {len(discontinued)}"
        )
        for name, n, last in sorted(discontinued, key=lambda x: -x[1]):
            print(f"    {name:<34} {n:>9,} rows, last {last}")
        print(
            f"  empty across the WHOLE panel (a real defect): {never or 'none'}"
        )


if __name__ == "__main__":
    main()
