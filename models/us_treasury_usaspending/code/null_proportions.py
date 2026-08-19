"""Report which columns the source leaves mostly empty, for schema.yml.

``not_null_proportion_multiple_columns`` fails on any column below the
threshold, and the USAspending transaction tables have dozens of FPDS fields
that are legitimately populated only for a small slice of awards. Rather than
guess that list, measure it against the built table and feed the result to
``build_dbt.py --sparse``.

The measurement covers the whole table, matching the test's scope. The test is
deliberately *not* scoped to the latest fiscal year: the shared
``not_null_proportion_multiple_columns`` macro introspects the where-subquery to
discover columns, and that introspection returns the *staging* column names, so
it breaks on any model that renames a column relative to staging (this one
renames ``*_unique_key`` to ``*_id``). Measuring over the full history is also
the stricter reading — a column populated in only a few years is not excused by
the latest year happening to carry it.

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
    client: bigquery.Client, project: str, table: str, at_least: float
) -> list[str]:
    cols = columns(table)
    ref = f"`{project}.{DATASET}.{table}`"
    parts = ",\n  ".join(
        f"countif({c} is not null) / count(*) as `{c}`" for c in cols
    )
    row = next(client.query(f"select\n  {parts}\nfrom {ref}").result())
    sparse = sorted(c for c in cols if (row[c] or 0) < at_least)
    print(
        f"{table}: {len(sparse)}/{len(cols)} columns below {at_least} "
        "over the full history"
    )
    return sparse


def sparse_from_parquet(
    table: str, at_least: float, data_dir: Path
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

    nulls: dict[str, int] = {}
    total = 0
    files = sorted((data_dir / "output" / table).rglob("*.parquet"))
    if not files:
        raise SystemExit(f"no parquet under {data_dir / 'output' / table}")
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

    sparse = sorted(
        staging_to_published.get(name, name)
        for name, n in nulls.items()
        if total and (1 - n / total) < at_least
    )
    print(
        f"{table}: {len(sparse)}/{len(nulls)} columns below {at_least} "
        f"over {total:,} rows (from parquet statistics)"
    )
    return sparse


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default="basedosdados-dev")
    ap.add_argument("--at-least", type=float, default=0.05)
    ap.add_argument("--tables", nargs="*", default=list(TRANSACTION_TABLES))
    ap.add_argument("--out", default=str(HERE / "sparse_columns.json"))
    ap.add_argument(
        "--from-parquet",
        action="store_true",
        help="read null counts from the local parquet footers instead of BigQuery",
    )
    args = ap.parse_args()

    out_path = Path(args.out)
    existing = json.loads(out_path.read_text()) if out_path.exists() else {}
    client = (
        None if args.from_parquet else bigquery.Client(project=args.project)
    )
    for table in args.tables:
        if args.from_parquet:
            existing[table] = sparse_from_parquet(
                table, args.at_least, DATA_DIR
            )
        else:
            existing[table] = sparse_for(
                client, args.project, table, args.at_least
            )
    out_path.write_text(json.dumps(existing, indent=1, sort_keys=True) + "\n")
    print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
