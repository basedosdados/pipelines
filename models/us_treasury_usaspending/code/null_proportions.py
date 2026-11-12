"""Report which columns the source leaves mostly empty, for schema.yml.

``not_null_proportion_multiple_columns`` fails on any column below the
threshold, and the USAspending transaction tables have dozens of FPDS fields
that are legitimately populated only for a small slice of awards. Rather than
guess that list, measure it against the built table and feed the result to
``build_dbt.py --sparse``.

The measurement must match the test's scope, so pass ``--fiscal-year`` with the
same year the test filters on. The test *is* scoped to the latest fiscal year
(``where: __most_recent_fiscal_year__``): the macro sums a CASE expression over
every column, so unscoped it scans the whole table — 364 GB across the two
transaction tables, enough to exhaust the project's daily query quota in a
single pass, every month, in each environment. Scoped to one fiscal year it
costs roughly a twentieth of that.

Scoping changes what is being asserted, and deliberately so: the recurring check
is "did the newest fiscal year arrive fully populated", which is the failure a
monthly refresh can actually introduce. A column that is dense overall but empty
in the newest year is exactly what should fail.

Pass several years and the exemption lists are unioned. Do that: a single year
is too brittle a basis for a test whose scope moves. Two things move it. The
newest fiscal year rolls over every 1 October, and for its first weeks the test
sees only that month — measured on FY2025, October adds 2 sparse columns to
contracts and none to assistance, and both extras are sparse in other years
anyway. Year-to-year drift does the rest: DUNS was retired for UEI in 2022 and
the COVID-19 supplementals have wound down, so those columns are dense in older
years and empty now. Exempting a column only withdraws an assertion about it, so
the union is the safe direction to err; unioning the recent complete years plus
the current one keeps the list stable without reaching back to years whose
reporting regime no longer applies.

Usage:
    GOOGLE_APPLICATION_CREDENTIALS=~/.basedosdados/credentials/staging.json \
      uv run python models/us_treasury_usaspending/code/null_proportions.py \
      --out models/us_treasury_usaspending/code/sparse_columns.json
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path

import pyarrow.parquet as pq
from google.cloud import bigquery

HERE = Path(__file__).resolve().parent
ARCH = HERE / "architecture"
DATASET = "us_treasury_usaspending"
PARTITION = "fiscal_year"
TRANSACTION_TABLES = ("contract_transaction", "assistance_transaction")
DATA_DIR = Path(
    os.environ.get(
        "USASPENDING_DATA_DIR",
        Path.home() / "Downloads" / "us_treasury_usaspending_data",
    )
)


def columns(table: str) -> list[str]:
    with (ARCH / f"{table}.csv").open() as f:
        return [r["name"] for r in csv.DictReader(f)]


def sparse_for(
    client: bigquery.Client,
    project: str,
    table: str,
    at_least: float,
    fiscal_years: list[int] | None,
) -> list[str]:
    cols = columns(table)
    ref = f"`{project}.{DATASET}.{table}`"
    parts = ",\n  ".join(
        f"countif({c} is not null) / count(*) as `{c}`" for c in cols
    )
    union: set[str] = set()
    for fy in fiscal_years or [None]:
        where = f"\nwhere {PARTITION} = {fy}" if fy is not None else ""
        row = next(
            client.query(f"select\n  {parts}\nfrom {ref}{where}").result()
        )
        sparse = {c for c in cols if (row[c] or 0) < at_least}
        scope = f"FY{fy}" if fy is not None else "the full history"
        print(
            f"{table}: {len(sparse)}/{len(cols)} columns below {at_least} over {scope}"
        )
        union |= sparse
    return sorted(union)


def sparse_from_parquet(
    table: str, at_least: float, data_dir: Path, fiscal_years: list[int] | None
) -> list[str]:
    """Null proportions read from the parquet footers, no BigQuery needed.

    Every column chunk records its null count, so the whole measurement is
    metadata — no scan, no query quota. The staging files carry the source
    column spelling, so results are mapped back to the published names.
    """
    import csv as _csv

    with (ARCH / f"{table}.csv").open() as f:
        rows = list(_csv.DictReader(f))
    staging_to_published = {
        (r["original_name"] if r["name"] != PARTITION else PARTITION): r[
            "name"
        ]
        for r in rows
    }

    root = data_dir / "output" / table
    patterns = (
        [f"{PARTITION}={fy}/*.parquet" for fy in fiscal_years]
        if fiscal_years
        else ["*.parquet"]
    )

    union: set[str] = set()
    n_cols = 0
    for pattern in patterns:
        files = sorted(root.rglob(pattern))
        if not files:
            raise SystemExit(f"no parquet matching {pattern} under {root}")
        nulls: dict[str, int] = {}
        total = 0
        for path in files:
            pf = pq.ParquetFile(path)
            names = pf.schema_arrow.names
            total += pf.metadata.num_rows
            for rg in range(pf.metadata.num_row_groups):
                group = pf.metadata.row_group(rg)
                for i, name in enumerate(names):
                    st = group.column(i).statistics
                    if st is not None:
                        nulls[name] = nulls.get(name, 0) + st.null_count
        n_cols = len(nulls)
        sparse = {
            staging_to_published.get(name, name)
            for name, n in nulls.items()
            if total and (1 - n / total) < at_least
        }
        scope = pattern.split("/")[0] if fiscal_years else "full history"
        print(
            f"{table}: {len(sparse)}/{n_cols} columns below {at_least} "
            f"over {total:,} rows, {scope} (from parquet statistics)"
        )
        union |= sparse

    if len(patterns) > 1:
        print(
            f"{table}: union across {len(patterns)} years -> {len(union)} columns"
        )
    return sorted(union)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default="basedosdados-dev")
    ap.add_argument("--at-least", type=float, default=0.05)
    ap.add_argument("--tables", nargs="*", default=list(TRANSACTION_TABLES))
    ap.add_argument("--out", default=str(HERE / "sparse_columns.json"))
    ap.add_argument(
        "--fiscal-year",
        type=int,
        nargs="*",
        dest="fiscal_years",
        help="measure these fiscal years and union the results, matching the "
        "test's where-scope; omit to measure the full history",
    )
    ap.add_argument(
        "--from-parquet",
        action="store_true",
        help="read null counts from the local parquet footers instead of BigQuery",
    )
    args = ap.parse_args()

    out_path = Path(args.out)
    existing = json.loads(out_path.read_text()) if out_path.exists() else {}
    for table in args.tables:
        if args.from_parquet:
            existing[table] = sparse_from_parquet(
                table, args.at_least, DATA_DIR, args.fiscal_years
            )
        else:
            client = bigquery.Client(project=args.project)
            existing[table] = sparse_for(
                client, args.project, table, args.at_least, args.fiscal_years
            )
    out_path.write_text(json.dumps(existing, indent=1, sort_keys=True) + "\n")
    print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
