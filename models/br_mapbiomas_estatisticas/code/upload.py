"""Upload the cleaned parquet to the basedosdados-dev staging data lake.

Run after `clean_data.py`. Publishing to the production dataset happens through
dbt and the table-approve action on merge, never from here.

    python models/br_mapbiomas_estatisticas/code/upload.py --workers 2
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import basedosdados as bd
from google.cloud import storage as gcs

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from pipelines.datasets.br_mapbiomas_estatisticas.constants import (  # noqa: E402
    constants,
)

DATASET_ID = constants.DATASET_ID.value
BILLING_PROJECT = "basedosdados-dev"
# Single source of truth, so adding a table cannot leave the uploader behind.
TABLES = constants.TABLES.value

# basedosdados-dev is a requester-pays bucket and the library never sets
# user_project. Patch it in at class level, before any bd.* call.
_orig_gcs_bucket = gcs.Client.bucket


def _bucket_with_user_project(self, bucket_name, user_project=None, **kwargs):
    return _orig_gcs_bucket(
        self,
        bucket_name,
        user_project=user_project or BILLING_PROJECT,
        **kwargs,
    )


gcs.Client.bucket = _bucket_with_user_project


def _delete_gcs_prefix(client, bucket_name: str, prefix: str) -> int:
    """Wipe the staging prefix before a replace.

    Without this, a re-run that produces a different partition set leaves the old
    objects behind and BigQuery reads a union of both.
    """
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))
    if blobs:
        bucket.delete_blobs(blobs)
    return len(blobs)


def upload_table_with_retry(
    table_id: str, output_root: Path, if_exists: str, attempts: int = 3
) -> str:
    """Retry a table upload on transient transport failures.

    `cobertura_municipio_classe` is 1,107 objects and the others are not much
    smaller; over that many requests GCS will occasionally close a connection
    (`RemoteDisconnected`), which fails the whole table. The retry re-uploads
    from scratch, which is safe because the mode is replace.
    """
    last: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return upload_table(table_id, output_root, if_exists)
        except Exception as exc:
            last = exc
            print(
                f"  {table_id}: attempt {attempt}/{attempts} failed: {exc}",
                flush=True,
            )
            if attempt < attempts:
                time.sleep(15 * attempt)
    raise RuntimeError(f"{table_id} failed after {attempts} attempts: {last}")


def upload_table(table_id: str, output_root: Path, if_exists: str) -> str:
    data_path = output_root / table_id
    if not data_path.exists():
        raise FileNotFoundError(data_path)

    if if_exists == "replace":
        storage = bd.Storage(dataset_id=DATASET_ID, table_id=table_id)
        removed = _delete_gcs_prefix(
            storage.client["storage_staging"],
            BILLING_PROJECT,
            f"staging/{DATASET_ID}/{table_id}/",
        )
        print(f"  {table_id}: deleted {removed} stale blobs", flush=True)

    storage = bd.Storage(dataset_id=DATASET_ID, table_id=table_id)
    storage.bucket._user_project = BILLING_PROJECT
    print(f"  {table_id}: uploading {data_path}", flush=True)
    storage.upload(path=str(data_path), mode="staging", if_exists=if_exists)

    print(f"  {table_id}: creating external table", flush=True)
    table = bd.Table(dataset_id=DATASET_ID, table_id=table_id)
    table.create(
        path=str(data_path),
        source_format="parquet",
        if_table_exists=if_exists,
        if_storage_data_exists="pass",
    )
    return f"{table_id}: done"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-root",
        default=str(
            Path.home()
            / "Downloads"
            / "br_mapbiomas_estatisticas_data"
            / "output"
        ),
    )
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--table", action="append", choices=TABLES)
    parser.add_argument("--if-exists", default="replace")
    args = parser.parse_args()

    output_root = Path(args.output_root)
    tables = args.table or TABLES
    print(f"uploading {tables} from {output_root} to {BILLING_PROJECT}")

    failures = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(upload_table, t, output_root, args.if_exists): t
            for t in tables
        }
        for future in as_completed(futures):
            table_id = futures[future]
            try:
                print(future.result(), flush=True)
            except Exception as exc:
                failures.append(table_id)
                print(f"ERROR {table_id}: {exc}", flush=True)

    if failures:
        raise SystemExit(f"failed: {failures}")
    print("all tables uploaded")


if __name__ == "__main__":
    os.environ.setdefault("GOOGLE_CLOUD_PROJECT", BILLING_PROJECT)
    main()
