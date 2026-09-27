"""Verify staging parquet: all-STRING schema, architecture column order, row counts.

The all-STRING check is the one that a green dev run cannot catch — a typed staging
column matches the dev table built from the same parquet and only fails once the
recurring pipeline recreates the table from a stringified header.
"""

import argparse
from pathlib import Path

import pyarrow.parquet as pq
from common import COMPLAINT, OUTPUT, load_cols


def verify(table_dir: Path, table: str) -> int:
    expected = [c.name for c in load_cols(table)]
    parts = sorted(table_dir.glob("*/data.parquet")) or sorted(
        table_dir.glob("data.parquet")
    )
    if not parts:
        raise SystemExit(f"no parquet found under {table_dir}")

    problems = []
    total = 0
    print(f"=== {table} ({len(parts)} partitions) ===")
    for p in parts:
        f = pq.ParquetFile(p)
        sch = f.schema_arrow
        rows = f.metadata.num_rows
        total += rows
        if list(sch.names) != expected:
            problems.append(
                f"{p}: column order/name mismatch -> {list(sch.names)}"
            )
        bad = [
            (n, str(t))
            for n, t in zip(sch.names, sch.types, strict=True)
            if str(t) != "string"
        ]
        if bad:
            problems.append(f"{p}: non-string columns {bad}")
        print(
            f"  {p.parent.name:12s} {rows:>10,}  {p.stat().st_size / 1e6:8.1f} MB"
        )

    print(f"  {'TOTAL':12s} {total:>10,}")
    if problems:
        print("\nPROBLEMS:")
        for x in problems:
            print("  " + x)
        raise SystemExit(1)
    print("  schema OK: all columns STRING, order matches the architecture")
    return total


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--output", type=Path, default=OUTPUT)
    ap.add_argument("--table", default=COMPLAINT)
    a = ap.parse_args()
    verify(a.output / a.table, a.table)
