"""Compare the cleaned Parquet against the raw source, column by column.

A ``try_cast`` returns NULL rather than raising when a value does not parse, so
a column whose source format the cast does not accept arrives empty while the
row count is unchanged and every dbt test still passes. The only way to catch
that is to count non-nulls on both sides and diff them.

For every column this reports the non-null count in the raw CSV and in the
cleaned Parquet. Any column where the cleaned side is lower is a cast that the
data outgrew, and the run exits non-zero.

    uv run --with duckdb python verify_clean.py contribution --cycle 1982
    uv run --with duckdb python verify_clean.py recipient
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import architecture as arch
import clean

SOURCES = {
    "recipient": "dime_recipients_all_1979_2024.csv.gz",
    "contributor": "dime_contributors_1979_2024.csv.gz",
}


def verify(table: str, cycle: int | None) -> bool:
    if table == "contribution":
        src = clean.INPUT / f"contribDB_{cycle}.csv.gz"
        out = clean.OUTPUT / "contribution" / str(cycle)
    else:
        src = clean.INPUT / SOURCES[table]
        out = clean.OUTPUT / table
    if not src.exists():
        raise FileNotFoundError(
            f"source {src} not present (already consumed?)"
        )
    files = sorted(out.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"no cleaned parquet under {out}")

    parallel = table == "contribution"
    con = clean._connect()
    header = clean._source_header(src)
    con.execute(
        "create or replace view raw as select * from "
        + clean._read_csv(src, header, parallel=parallel)
    )
    con.execute(
        f"create or replace view built as select * from read_parquet('{out}/*.parquet')"
    )

    raw_n = con.execute("select count(*) from raw").fetchone()[0]
    built_n = con.execute("select count(*) from built").fetchone()[0]
    print(
        f"rows: raw {raw_n:,}  built {built_n:,}  "
        f"{'MATCH' if raw_n == built_n else 'MISMATCH'}"
    )

    cols = [c for c in arch.TABLES[table] if c[9] and "<" not in c[9]]
    # One aggregate pass per side rather than a query per column.
    raw_sel = ", ".join(
        f"""count(case when trim("{c[9]}") not in ('', '\\N') then 1 end) as "{c[0]}\""""
        for c in cols
    )
    built_sel = ", ".join(f'count("{c[0]}") as "{c[0]}"' for c in cols)
    raw_counts = con.execute(f"select {raw_sel} from raw").fetchone()
    built_counts = con.execute(f"select {built_sel} from built").fetchone()

    print(f"\n{'column':<38} {'type':<8} {'raw':>12} {'built':>12}  status")
    ok = raw_n == built_n
    for col, r, b in zip(cols, raw_counts, built_counts, strict=True):
        name, bq_type = col[0], col[1]
        if b < r:
            status = f"LOSS {r - b:,}"
            ok = False
        elif b > r:
            status = "gained (check null tokens)"
        elif r == 0:
            status = "empty in source"
        else:
            status = "ok"
        print(f"{name:<38} {bq_type:<8} {r:>12,} {b:>12,}  {status}")
    con.close()
    return ok


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "table", choices=["contribution", "recipient", "contributor"]
    )
    p.add_argument("--cycle", type=int)
    args = p.parse_args()
    ok = verify(args.table, args.cycle)
    print(
        "\nRESULT:",
        "OK — no column lost values to a cast" if ok else "CAST LOSS DETECTED",
    )
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
