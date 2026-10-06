"""
Upload one year's partitioned CSVs to the dev staging bucket, replacing that
year's prefix only.

The staging external tables map CSV columns BY POSITION, so every file is
reordered to the live staging schema (read from BigQuery) before upload, and
the upload refuses any file whose columns are not exactly that schema.

Usage:
    TSE_DATA_DIR=... python -m models.br_tse_eleicoes.code.python.upload_year 2026 table [table ...]
    TSE_DATA_DIR=... python -m models.br_tse_eleicoes.code.python.upload_year --create new_table [...]
"""

import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
from google.cloud import bigquery, storage
from google.oauth2 import service_account

from models.br_tse_eleicoes.code.python.config import OUTPUT_PYTHON

PROJECT = "basedosdados-dev"
BUCKET = "basedosdados-dev"
DATASET = "br_tse_eleicoes"
CRED = Path(
    os.environ.get(
        "BD_SERVICE_ACCOUNT_DEV",
        Path.home() / ".basedosdados/credentials/staging.json",
    )
)


def _staging_schema(bq: bigquery.Client, table: str) -> list[str]:
    q = f"""SELECT column_name FROM `{PROJECT}.{DATASET}_staging.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = @t ORDER BY ordinal_position"""
    job = bq.query(
        q,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("t", "STRING", table)
            ]
        ),
    )
    return [r.column_name for r in job.result()]


def upload(ano: int, tables: list[str], dry_run: bool = False) -> None:
    cred = service_account.Credentials.from_service_account_file(str(CRED))
    bq = bigquery.Client(project=PROJECT, credentials=cred)
    bucket = storage.Client(project=PROJECT, credentials=cred).bucket(
        BUCKET, user_project=PROJECT
    )
    for table in tables:
        schema = _staging_schema(bq, table)
        root = OUTPUT_PYTHON / table / f"ano={ano}"
        files = sorted(
            f
            for f in root.rglob("*.csv")
            if not f.name.endswith(".staging.csv")
        )
        payloads = []
        # The seção giants are staged as parquet (columns matched by name,
        # not position): upload them as written.
        for f in sorted(root.rglob("data.parquet")):
            rel = f.relative_to(OUTPUT_PYTHON / table)
            payloads.append((f"staging/{DATASET}/{table}/{rel.as_posix()}", f))
        if not files and not payloads:
            print(f"{table}: no files for ano={ano}, skipped")
            continue
        for f in files:
            # Hive keys (ano=, sigla_uf=) live in the path, not in the file
            rel = f.relative_to(OUTPUT_PYTHON / table)
            hive = {part.split("=", 1)[0] for part in rel.parts[:-1]}
            cols = [c for c in schema if c not in hive]
            header = list(pd.read_csv(f, nrows=0).columns)
            if set(header) != set(cols):
                msg = (
                    f"{f}: columns differ from staging {table}. "
                    f"missing={set(cols) - set(header)} "
                    f"extra={set(header) - set(cols)}"
                )
                raise ValueError(msg)
            src = f
            if header != cols:  # reorder to the staging position order
                src = f.with_suffix(".staging.csv")
                first = True
                for chunk in pd.read_csv(
                    f, dtype=str, keep_default_na=False, chunksize=1_000_000
                ):
                    chunk[cols].to_csv(
                        src,
                        index=False,
                        header=first,
                        mode="w" if first else "a",
                    )
                    first = False
            payloads.append(
                (f"staging/{DATASET}/{table}/{rel.as_posix()}", src)
            )
        size = sum(p.stat().st_size for _, p in payloads) / 1e6
        print(f"{table}: {len(payloads)} files, {size:,.0f} MB")
        if dry_run:
            continue
        prefix = f"staging/{DATASET}/{table}/ano={ano}/"
        stale = list(bucket.list_blobs(prefix=prefix))
        for blob in stale:
            blob.delete()
        for name, src in payloads:
            # gcloud does resumable/parallel uploads; the python client hung
            # on, or timed out, the 300 MB files
            subprocess.run(
                ["gcloud", "storage", "cp", "-q", f"--billing-project={PROJECT}",
                 str(src), f"gs://{BUCKET}/{name}"],
                check=True,
                env={**os.environ, "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE": str(CRED)},
            )  # fmt: skip
            print(f"    {name}", flush=True)
        print(f"  replaced {len(stale)} old blobs under {prefix}")


def create_staging(tables: list[str]) -> None:
    """Create a NEW staging table from every year's output (all-STRING CSV).

    For tables that have no staging table yet; existing ones go through
    ``upload`` so the other years stay untouched.
    """
    import basedosdados as bd

    orig = storage.Client.bucket

    def bucket(self, name, user_project=None):  # requester-pays bucket
        return orig(self, name, user_project=PROJECT)

    storage.Client.bucket = bucket
    for table in tables:
        bd.Table(dataset_id=DATASET, table_id=table).create(
            path=str(OUTPUT_PYTHON / table),
            source_format="csv",
            if_table_exists="replace",
            if_storage_data_exists="replace",
            if_dataset_exists="pass",
        )
        print(f"{table}: staging table created")


if __name__ == "__main__":
    args = sys.argv[1:]
    if args[0] == "--create":
        create_staging(args[1:])
    else:
        dry = "--dry-run" in args
        args = [a for a in args if a != "--dry-run"]
        upload(int(args[0]), args[1:], dry_run=dry)
