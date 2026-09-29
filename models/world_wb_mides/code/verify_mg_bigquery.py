"""Verify the built BigQuery tables against the models -- WITHOUT spending quota.

    ~/.venvs/bd-pipelines/bin/python models/world_wb_mides/code/verify_mg_bigquery.py

THIS READS THE CATALOG, NOT THE TABLES. `__TABLES__` bills exactly zero;
`INFORMATION_SCHEMA.COLUMNS` bills about 10 MB, which is not zero but is five
orders of magnitude below a rebuild (~600 GiB). Every statement is dry-run
first and refused if it exceeds BUDGET_BYTES, so a future edit that accidentally
scans real data fails loudly instead of quietly costing a day's quota. The run
prints what it actually spent rather than asserting it was free.

WHAT IT CAN AND CANNOT SEE
--------------------------
Catalog metadata answers: does the table exist, how many rows does it have, what
are its columns and their types, when was it last modified, is it partitioned on
the right field. That covers the failure modes that have actually bitten this
onboarding -- a model that silently dropped a year, a type that came out STRING,
a column that vanished, a table that reported ERROR but had really built.

It cannot answer anything about VALUES: whether accents survived, whether a key
is unique, whether the MG arm actually reaches 2026. Those need real scans. Key
uniqueness is already proven on the parquet (free, and stronger -- it runs on
every row rather than a sample), and the rest waits for the rebuild.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from google.cloud import bigquery
from google.oauth2 import service_account

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# pyrefly: ignore [missing-import]  # sibling module via sys.path
import register_mg_metadata as reg

CREDENTIALS = Path.home() / ".basedosdados/credentials/staging.json"
PROJECT = "basedosdados-dev"
DATASET = "world_wb_mides"
EXPECTED_END_YEAR = reg.END_YEAR

# The six tables that already carried MG, plus the 43 new ones.
SHARED = [
    "empenho",
    "liquidacao",
    "pagamento",
    "licitacao",
    "licitacao_item",
    "licitacao_participante",
]

# Catalog reads only. Anything above this is a table scan that crept in.
BUDGET_BYTES = 200 * 1024 * 1024
_spent = 0


def catalog_query(client: bigquery.Client, sql: str) -> list:
    """Run `sql` only after proving it reads the catalog, not the tables."""
    global _spent
    dry = client.query(sql, job_config=bigquery.QueryJobConfig(dry_run=True))
    estimate = dry.total_bytes_processed or 0
    if estimate > BUDGET_BYTES:
        raise SystemExit(
            f"REFUSING to run: BigQuery estimates {estimate / 1e9:.2f} GB, over "
            f"the {BUDGET_BYTES / 1e6:.0f} MB catalog budget. This script must "
            f"not scan table data.\n{sql[:200]}"
        )
    _spent += estimate
    return list(client.query(sql).result())


def main() -> None:
    creds = service_account.Credentials.from_service_account_file(
        str(CREDENTIALS)
    )
    info = json.loads(CREDENTIALS.read_text())
    client = bigquery.Client(credentials=creds, project=info["project_id"])

    mg_dir = Path(__file__).resolve().parent.parent / "mg"
    new_tables = sorted(
        p.stem[len("world_wb_mides__") :] for p in mg_dir.glob("*.sql")
    )

    rows = catalog_query(
        client,
        f"select table_id, row_count, size_bytes, last_modified_time "
        f"from `{PROJECT}.{DATASET}.__TABLES__`",
    )
    built = {r.table_id: r for r in rows}

    cols = catalog_query(
        client,
        f"select table_name, column_name, data_type, is_partitioning_column "
        f"from `{PROJECT}.{DATASET}.INFORMATION_SCHEMA.COLUMNS`",
    )
    by_table: dict[str, dict[str, tuple[str, str]]] = {}
    for c in cols:
        by_table.setdefault(c.table_name, {})[c.column_name] = (
            c.data_type,
            c.is_partitioning_column,
        )

    # BigQuery reports INT64/FLOAT64/STRING/DATE; the models say the same words.
    problems: list[str] = []
    total_rows = 0

    print(f"{'table':<36} {'rows':>14}  {'cols':>5}  status")
    for slug in new_tables:
        table = built.get(slug)
        if not table:
            problems.append(f"{slug}: NOT BUILT in {PROJECT}.{DATASET}")
            print(f"{slug:<36} {'-':>14}  {'-':>5}  NOT BUILT")
            continue
        total_rows += table.row_count

        want = dict(
            reg.typed_columns(str(mg_dir / f"world_wb_mides__{slug}.sql"))
        )
        got = by_table.get(slug, {})
        missing = sorted(set(want) - set(got))
        extra = sorted(set(got) - set(want))
        if missing:
            problems.append(
                f"{slug}: columns in the model but NOT in BigQuery: {missing}"
            )
        if extra:
            problems.append(
                f"{slug}: columns in BigQuery but NOT in the model: {extra}"
            )
        for name, expected in want.items():
            actual = got.get(name)
            if actual and actual[0] != expected:
                problems.append(
                    f"{slug}.{name}: BigQuery has {actual[0]}, model says {expected}"
                )

        partitioned = sorted(n for n, (_, p) in got.items() if p == "YES")
        if partitioned != ["ano"]:
            problems.append(
                f"{slug}: partitioned on {partitioned}, expected ['ano']"
            )

        flag = "" if not (missing or extra) else "  <-- schema drift"
        print(f"{slug:<36} {table.row_count:>14,}  {len(got):>5}{flag}")

    print(f"\n{'TOTAL (43 new tables)':<36} {total_rows:>14,}")

    print(f"\n{'shared table':<36} {'rows':>14}  last modified")
    for slug in SHARED:
        table = built.get(slug)
        if not table:
            problems.append(f"{slug}: NOT BUILT")
            continue
        import datetime

        when = datetime.datetime.fromtimestamp(table.last_modified_time / 1000)
        print(f"{slug:<36} {table.row_count:>14,}  {when:%Y-%m-%d %H:%M}")

    print()
    if problems:
        print(f"{len(problems)} problems:")
        for p in problems:
            print(f"  - {p}")
        print(f"\n{_spent / 1e6:.1f} MB billed")
        raise SystemExit(1)
    print(f"no schema discrepancies; {_spent / 1e6:.1f} MB billed")


if __name__ == "__main__":
    main()
