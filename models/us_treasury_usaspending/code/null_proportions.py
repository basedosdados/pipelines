"""Report which columns the source leaves mostly empty, for schema.yml.

``not_null_proportion_multiple_columns`` fails on any column below the
threshold, and the USAspending transaction tables have dozens of FPDS fields
that are legitimately populated only for a small slice of awards. Rather than
guess that list, measure it against the built table and feed the result to
``build_dbt.py --sparse``.

The measurement uses the same filter the test does — the most recent fiscal
year — so the two agree.

Usage:
    GOOGLE_APPLICATION_CREDENTIALS=~/.basedosdados/credentials/staging.json \
      uv run python models/us_treasury_usaspending/code/null_proportions.py \
      --out models/us_treasury_usaspending/code/sparse_columns.json
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

from google.cloud import bigquery

HERE = Path(__file__).resolve().parent
ARCH = HERE / "architecture"
DATASET = "us_treasury_usaspending"
PARTITION = "fiscal_year"
TRANSACTION_TABLES = ("contract_transaction", "assistance_transaction")


def columns(table: str) -> list[str]:
    with (ARCH / f"{table}.csv").open() as f:
        return [r["name"] for r in csv.DictReader(f)]


def sparse_for(
    client: bigquery.Client, project: str, table: str, at_least: float
) -> list[str]:
    cols = columns(table)
    ref = f"`{project}.{DATASET}.{table}`"
    max_fy = next(
        client.query(f"select max({PARTITION}) m from {ref}").result()
    ).m
    if max_fy is None:
        raise SystemExit(f"{table} is empty")
    parts = ",\n  ".join(
        f"countif({c} is not null) / count(*) as `{c}`" for c in cols
    )
    q = f"select\n  {parts}\nfrom {ref} where {PARTITION} = {max_fy}"
    row = next(client.query(q).result())
    sparse = sorted(c for c in cols if (row[c] or 0) < at_least)
    print(
        f"{table}: fiscal_year={max_fy}, {len(sparse)}/{len(cols)} columns below {at_least}"
    )
    return sparse


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default="basedosdados-dev")
    ap.add_argument("--at-least", type=float, default=0.05)
    ap.add_argument("--tables", nargs="*", default=list(TRANSACTION_TABLES))
    ap.add_argument("--out", default=str(HERE / "sparse_columns.json"))
    args = ap.parse_args()

    client = bigquery.Client(project=args.project)
    out_path = Path(args.out)
    existing = json.loads(out_path.read_text()) if out_path.exists() else {}
    for table in args.tables:
        existing[table] = sparse_for(
            client, args.project, table, args.at_least
        )
    out_path.write_text(json.dumps(existing, indent=1, sort_keys=True) + "\n")
    print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
