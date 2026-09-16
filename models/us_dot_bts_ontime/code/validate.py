"""Check the materialized flight table against the local parquet, column by column.

    uv run --no-project --with basedosdados --with pyarrow --with pandas \
        python models/us_dot_bts_ontime/code/validate.py 2026

`safe_cast` returns NULL rather than raising, so a column whose type was assigned
wrongly arrives *empty* while every dbt test still passes. Row counts do not catch
it — only a per-column non-null comparison does.

The comparison is scoped to one year so the query hits a single partition; an
unscoped version would scan the whole 232M-row table.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import basedosdados as bd
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.us_dot_bts_ontime.utils import read_arch

BILLING = "basedosdados-dev"
OUTPUT = (
    Path(
        os.environ.get(
            "BTS_DATA_DIR",
            Path.home() / "Downloads" / "us_dot_bts_ontime_data",
        )
    )
    / "output"
)
# A tolerance is needed only because the parquet side counts the empty string as a
# value in a couple of source columns while BigQuery reads it as NULL; anything
# larger than this is a real cast failure, not a representation difference.
TOLERANCE = 0.0001


def parquet_nonnull(year: int) -> tuple[dict[str, int], int]:
    """Non-null count per column for one year, read from the parquet footers."""
    files = sorted((OUTPUT / "flight" / f"year={year}").glob("data_*.parquet"))
    if not files:
        raise SystemExit(f"no parquet for year={year}")
    counts: dict[str, int] = {}
    rows = 0
    for f in files:
        md = pq.ParquetFile(f).metadata
        rows += md.num_rows
        schema = pq.ParquetFile(f).schema_arrow
        for rg in range(md.num_row_groups):
            for i, name in enumerate(schema.names):
                c = md.row_group(rg).column(i)
                counts[name] = counts.get(name, 0) + (
                    c.num_values - c.statistics.null_count
                )
    return counts, rows


def bq_nonnull(year: int, columns: list[str]) -> dict[str, int]:
    exprs = ",\n  ".join(f"countif({c} is not null) as {c}" for c in columns)
    sql = (
        f"select {exprs}\n"
        f"from `{BILLING}.us_dot_bts_ontime.flight`\n"
        f"where year = {year}"
    )
    df = bd.read_sql(sql, billing_project_id=BILLING, from_file=True)
    return {c: int(df[c].iloc[0]) for c in columns}


def main(year: int) -> None:
    arch = read_arch("flight")
    derived = {a["name"] for a in arch if "Derived" in a["observations_en"]}
    columns = [a["name"] for a in arch]

    local, rows = parquet_nonnull(year)
    remote = bq_nonnull(year, columns)
    print(f"year={year}: {rows:,} rows in parquet\n")

    bad = []
    for c in columns:
        lo, ro = local.get(c, 0), remote.get(c, 0)
        if lo == ro:
            continue
        drift = abs(lo - ro) / max(rows, 1)
        flag = "OK(derived)" if c in derived and ro >= lo else "MISMATCH"
        if drift > TOLERANCE and flag == "MISMATCH":
            bad.append((c, lo, ro, drift))
        print(
            f"  {c:<40} parquet={lo:>9,}  bq={ro:>9,}  drift={drift:.6f}  {flag}"
        )

    if bad:
        print(f"\n{len(bad)} column(s) lost data in the cast:")
        for c, lo, ro, drift in bad:
            print(f"  {c}: parquet {lo:,} -> bq {ro:,} ({drift:.4%})")
        raise SystemExit(1)
    print(f"\nall {len(columns)} columns match within {TOLERANCE:.4%}")


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 2026)
