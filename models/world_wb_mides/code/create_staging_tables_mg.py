"""Create the BigQuery external tables over the MG staging mirrors.

WHY THIS IS A SEPARATE SCRIPT
-----------------------------
`upload_mg.py` deliberately only copies objects into
`gs://basedosdados-dev/staging/world_wb_mides/<mirror>/`. That is enough for the
four mirrors MiDES already had, whose external tables were created years ago by
the original ingest. The 45 mirrors added by the full-source onboarding have no
table at all, and an object under a prefix with no table is invisible to dbt --
the failure reads as `Table ... was not found`, which looks like a typo.

SHAPE
-----
Matches `raw_rsp_mg` exactly, which is the contract the four existing mirrors
already meet: EXTERNAL, PARQUET, one wildcard URI, **every column STRING**, no
autodetect and no hive partitioning. All-STRING is the house convention for
staging (see `.claude/rules/bigquery-conventions.md`); the dbt model does the
typing. Column names and order come from `clean_mg.SPECS`, the same source the
parquet writer uses, so the table cannot drift from the files.

Existing tables are left alone unless `--replace` is passed. Replacing is safe --
an external table holds no data -- but it would also silently rewrite the four
mirrors the published spend tables depend on, so it is opt-in.

Usage:
    python create_staging_tables_mg.py --dry-run
    python create_staging_tables_mg.py
    python create_staging_tables_mg.py --replace --mirror raw_contrato_mg
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from google.cloud import bigquery
from google.oauth2 import service_account

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# pyrefly: ignore [missing-import]  # sibling module via sys.path
import clean_mg

CREDENTIALS = Path.home() / ".basedosdados/credentials/staging.json"
DATASET = "world_wb_mides_staging"
BUCKET = "basedosdados-dev"
PREFIX = "staging/world_wb_mides"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--replace", action="store_true")
    parser.add_argument("--mirror", action="append")
    args = parser.parse_args()

    info = json.loads(CREDENTIALS.read_text())
    creds = service_account.Credentials.from_service_account_file(
        str(CREDENTIALS)
    )
    client = bigquery.Client(credentials=creds, project=info["project_id"])

    existing = {
        t.table_id
        for t in client.list_tables(f"{info['project_id']}.{DATASET}")
    }

    created = skipped = 0
    for phase, mirror in sorted(clean_mg.MIRROR.items()):
        if args.mirror and mirror not in args.mirror:
            continue
        columns = [name for name, _, _ in clean_mg.SPECS[phase]]
        if mirror in existing and not args.replace:
            print(f"  {mirror:<42} exists, skipping ({len(columns)} cols)")
            skipped += 1
            continue

        config = bigquery.ExternalConfig("PARQUET")
        config.source_uris = [f"gs://{BUCKET}/{PREFIX}/{mirror}/*"]
        table = bigquery.Table(f"{info['project_id']}.{DATASET}.{mirror}")
        table.schema = [bigquery.SchemaField(c, "STRING") for c in columns]
        table.external_data_configuration = config

        if args.dry_run:
            print(f"  {mirror:<42} WOULD CREATE  {len(columns)} cols")
        else:
            client.delete_table(table, not_found_ok=True)
            client.create_table(table)
            print(f"  {mirror:<42} created       {len(columns)} cols")
        created += 1

    print(f"\n{created} created, {skipped} already present")


if __name__ == "__main__":
    main()
