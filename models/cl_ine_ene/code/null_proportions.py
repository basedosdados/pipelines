#!/usr/bin/env python3
"""Measure per-column null proportions from the parquet footers.

`not_null_proportion_multiple_columns` reads every column of the model, so on a
271-column, 20M-row table an unscoped run is expensive enough to matter — and it
runs at `dbt compile`, not just `dbt test`. The house fix is to scope it to the
most recent period and exempt the columns that are legitimately sparse.

Those exemptions are measured here from the parquet footers, which record a
`null_count` per column chunk: metadata only, no scan, no BigQuery quota. The
exemption set is the UNION across the most recent periods, not one period's
measurement, because the scope rolls forward and a column sitting near the floor
crosses it later.

    python models/cl_ine_ene/code/null_proportions.py --periods 12
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib

import pyarrow.parquet as pq

DATA = pathlib.Path(
    os.environ.get(
        "CL_INE_ENE_DATA", pathlib.Path.home() / "Downloads/cl_ine_ene_data"
    )
)
TABLE_DIR = DATA / "output" / "microdato"


def proportions(path: pathlib.Path) -> dict[str, float]:
    """Non-null proportion per column, read from the footer."""
    metadata = pq.ParquetFile(path).metadata
    rows = metadata.num_rows
    nulls: dict[str, int] = {}
    for group in range(metadata.num_row_groups):
        row_group = metadata.row_group(group)
        for index in range(row_group.num_columns):
            column = row_group.column(index)
            name = column.path_in_schema
            nulls[name] = nulls.get(name, 0) + column.statistics.null_count
    return {name: (rows - count) / rows for name, count in nulls.items()}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--periods",
        type=int,
        default=12,
        help="how many of the most recent periods to union over",
    )
    parser.add_argument("--at-least", type=float, default=0.05)
    args = parser.parse_args()

    files = sorted(TABLE_DIR.glob("ano=*/mes=*/data.parquet"))[-args.periods :]
    if not files:
        raise SystemExit(f"no parquet under {TABLE_DIR}")

    sparse: dict[str, float] = {}
    for path in files:
        for name, share in proportions(path).items():
            if share < args.at_least:
                sparse[name] = min(share, sparse.get(name, 1.0))

    def period(path: pathlib.Path) -> str:
        return f"{path.parts[-3].split('=')[1]}-{path.parts[-2].split('=')[1]}"

    print(
        f"union over {len(files)} periods ({period(files[0])} .. {period(files[-1])}), "
        f"floor {args.at_least}"
    )
    print(f"{len(sparse)} columns below the floor in at least one period\n")
    for name, share in sorted(sparse.items(), key=lambda kv: kv[1]):
        print(f"  {name:28} {share * 100:6.2f}% non-null")

    out = DATA / "sparse_columns.json"
    out.write_text(json.dumps(sorted(sparse), indent=1) + "\n")
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
