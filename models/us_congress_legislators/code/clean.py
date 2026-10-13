"""Clean unitedstates/congress-legislators YAML into 8 Data Basis tables.

Source: https://github.com/unitedstates/congress-legislators (CC0).
Output: one unpartitioned snappy Parquet per table under output/<table>/data.parquet.
All columns are written as STRING; dbt applies safe_cast to final types.

The transform lives in pipelines/datasets/us_congress_legislators/utils.py, shared
with the recurring Prefect pipeline.
"""

import pathlib
import sys

from pipelines.datasets.us_congress_legislators.utils import (
    BUILDERS,
    write_table,
)

HERE = pathlib.Path(__file__).resolve().parent
INPUT = HERE / "input"
OUTPUT = HERE.parent / "output"


def main():
    targets = sys.argv[1:] or list(BUILDERS)
    print("Cleaning congress-legislators tables:")
    for slug in targets:
        if slug not in BUILDERS:
            print(f"  ! unknown table {slug}")
            continue
        df = BUILDERS[slug](INPUT)
        out = write_table(df, OUTPUT / slug)
        print(
            f"  {slug:22s} {len(df):>6d} rows  {len(df.columns):>2d} cols -> {out}"
        )


if __name__ == "__main__":
    main()
