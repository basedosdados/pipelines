"""Measure non-null proportions per column, per year, from parquet footers.

Every parquet column chunk records ``null_count``, so this is metadata only: no
BigQuery scan and no quota spend. Used to choose the ``ignore_values`` exemption
list for ``not_null_proportion_multiple_columns``, which is scoped to the most
recent year on this table.
"""

import argparse
from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq
from common import COMPLAINT, OUTPUT


def measure(table_dir: Path) -> dict[int, dict[str, tuple[int, int]]]:
    """Return {year: {column: (non_null, total)}}."""
    out: dict[int, dict[str, tuple[int, int]]] = {}
    for p in sorted(table_dir.glob("year=*/data.parquet")):
        year = int(p.parent.name.split("=", 1)[1])
        md = pq.ParquetFile(p).metadata
        names = md.schema.names
        nulls: dict[str, int] = defaultdict(int)
        for rg in range(md.num_row_groups):
            g = md.row_group(rg)
            for c in range(g.num_columns):
                col = g.column(c)
                nulls[names[c]] += col.statistics.null_count
        out[year] = {n: (md.num_rows - nulls[n], md.num_rows) for n in names}
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--output", type=Path, default=OUTPUT)
    ap.add_argument("--at-least", type=float, default=0.05)
    a = ap.parse_args()

    per_year = measure(a.output / COMPLAINT)
    years = sorted(per_year)
    latest = years[-1]

    cols = list(per_year[latest])
    print("non-null proportion by column (rows per year in header)\n")
    head = "".join(f"{y:>9}" for y in years)
    print(f"{'column':32s}{head}")
    for col in cols:
        row = "".join(
            f"{per_year[y][col][0] / max(per_year[y][col][1], 1):>9.3f}"
            for y in years
        )
        print(f"{col:32s}{row}")

    print(
        f"\nmost recent year = {latest} ({per_year[latest][cols[0]][1]:,} rows)"
    )
    fail = [
        c
        for c in cols
        if per_year[latest][c][0] / max(per_year[latest][c][1], 1) < a.at_least
    ]
    print(f"columns below at_least={a.at_least} in {latest}: {fail or 'none'}")

    # Union across the three most recent years: the test scope rolls over on
    # 1 January, and exempting a column only withdraws an assertion about it, so
    # the union is the safe direction to err.
    union = sorted(
        {
            c
            for y in years[-3:]
            for c in cols
            if per_year[y][c][0] / max(per_year[y][c][1], 1) < a.at_least
        }
    )
    print(f"union over {years[-3:]}: {union or 'none'}")


if __name__ == "__main__":
    main()
