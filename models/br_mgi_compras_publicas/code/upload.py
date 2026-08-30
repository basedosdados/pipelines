"""Upload the cleaned parquet of br_mgi_compras_publicas to BigQuery dev staging.

    uv run python models/br_mgi_compras_publicas/code/upload.py            # every table
    uv run python models/br_mgi_compras_publicas/code/upload.py contratacao

Targets basedosdados-dev only. Prod table data is never uploaded from here: it is
materialised by the GitHub table-approve action when the onboarding PR merges
(.claude/rules/onboarding-workflow.md).

Each table's BigQuery row count is checked against the local parquet count and the
run stops at the first mismatch, so a partial upload cannot pass for a good one.

Requires ~/.basedosdados/config.toml. The GCS bucket is requester-pays, so
gcs.Client.bucket is monkeypatched to pin user_project to the billing project.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import basedosdados as bd
import google.cloud.storage as gcs
import pyarrow as pa
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from dbt_spec import TABLES  # noqa: E402

from pipelines.datasets.br_mgi_compras_publicas.utils import (  # noqa: E402
    load_architecture,
    string_schema,
)

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "br_mgi_compras_publicas"

#: Tables large enough that bd's single-threaded resumable upload is a liability.
#: gcloud storage rsync is parallel, retries internally and skips blobs already
#: present, so an interruption costs one file rather than the whole transfer.
RSYNC_THRESHOLD_BYTES = 500 * 1024**2

_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket


def output_dir() -> Path:
    root = Path(
        os.environ.get(
            "COMPRAS_DATA_DIR",
            Path.home() / "Downloads" / "br_mgi_compras_publicas_data",
        )
    )
    return root / "output"


def _credentials_path() -> str:
    """Path to the service-account key basedosdados is configured to use.

    Read from ~/.basedosdados/config.toml so gcloud and the python client cannot
    drift onto different identities. The file itself is never opened here, only
    its path handed to gcloud.
    """
    try:  # stdlib since 3.11; tomli is the backport and is not a hard dep
        import tomllib as toml_reader
    except ModuleNotFoundError:  # pragma: no cover
        import tomli as toml_reader

    cfg = toml_reader.loads(
        (Path.home() / ".basedosdados" / "config.toml").read_text()
    )
    return cfg["gcloud-projects"]["staging"]["credentials_path"]


def local_rows(table: str) -> tuple[int, int, int]:
    files = sorted((output_dir() / table).rglob("*.parquet"))
    rows = sum(pq.ParquetFile(f).metadata.num_rows for f in files)
    size = sum(f.stat().st_size for f in files)
    return rows, len(files), size


def seed_header(table: str) -> Path | None:
    """Write a 0-row parquet that sorts first inside the table's prefix.

    The table-approve action derives a staging table's schema with
    `pd.read_parquet` on the first blob in GCS lexicographic order, which loads
    that entire file into pandas just to read column names. On a multi-GB
    parquet that OOM-kills the CI runner and no prod table is ever built. A
    0-row file named `00_header.parquet` inside the lexicographically first
    partition is picked instead, costs nothing to read, and adds no rows because
    the external table is a wildcard over the whole prefix.
    """
    root = output_dir() / table
    partitions = sorted(p for p in root.glob("*=*") if p.is_dir())
    target_dir = partitions[0] if partitions else root
    path = target_dir / "00_header.parquet"
    if path.exists():
        return path
    schema = string_schema(load_architecture(table))
    # The partition column is encoded in the directory name, so it must not also
    # appear as a column in the file.
    partition_col = target_dir.name.split("=")[0] if partitions else None
    fields = [f for f in schema if f.name != partition_col]
    pq.write_table(
        pa.Table.from_pylist([], schema=pa.schema(fields)),
        path,
        compression="snappy",
    )
    print(f"[{table}] seeded 0-row header at {path.relative_to(root.parent)}")
    return path


def rsync_to_gcs(table: str) -> None:
    src = str(output_dir() / table)
    dest = f"gs://{BILLING_PROJECT}/staging/{DATASET_ID}/{table}"
    print(f"[{table}] rsync {src} -> {dest}")
    # Drive gcloud with the same service account the python client uses, rather
    # than whatever user credentials happen to be cached: those expire, and when
    # they do gcloud fails with "Reauthentication failed. cannot prompt during
    # non-interactive execution", which a script cannot recover from.
    env = {
        **os.environ,
        "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE": _credentials_path(),
    }
    subprocess.run(
        [
            "gcloud",
            "storage",
            "rsync",
            "-r",
            f"--billing-project={BILLING_PROJECT}",
            src,
            dest,
        ],
        check=True,
        env=env,
    )


def bq_rows(table: str) -> int:
    result = bd.read_sql(
        f"select count(*) as n from `{BILLING_PROJECT}.{DATASET_ID}_staging.{table}`",
        billing_project_id=BILLING_PROJECT,
        from_file=True,
    )
    return int(result["n"].iloc[0])


def upload_table(table: str) -> None:
    path = output_dir() / table
    if not path.exists():
        raise SystemExit(
            f"{table}: nothing at {path} — run download_and_clean.py --consolidate"
        )

    expected, nfiles, size = local_rows(table)
    print(
        f"[{table}] local: {expected:,} rows in {nfiles} file(s), {size / 1e9:.2f} GB"
    )
    seed_header(table)

    bq_table = bd.Table(dataset_id=DATASET_ID, table_id=table)
    if size >= RSYNC_THRESHOLD_BYTES:
        rsync_to_gcs(table)
        bq_table.create(
            path=str(path),
            source_format="parquet",
            if_table_exists="replace",
            if_storage_data_exists="pass",
            if_dataset_exists="pass",
        )
    else:
        bq_table.create(
            path=str(path),
            source_format="parquet",
            if_table_exists="replace",
            if_storage_data_exists="replace",
            if_dataset_exists="pass",
        )

    actual = bq_rows(table)
    if actual != expected:
        raise SystemExit(
            f"{table}: BigQuery has {actual:,} rows but the local parquet has "
            f"{expected:,} — refusing to continue"
        )
    print(f"[{table}] uploaded and verified: {actual:,} rows")


def upload_with_retry(table: str, attempts: int = 4) -> None:
    """Upload one table, retrying transient network failures.

    Long uploads run for minutes and the underlying client does not retry: a
    single `RemoteDisconnected` partway through a multi-table run aborted it and
    left the remaining tables untouched. The upload itself is idempotent -- the
    staging prefix and table are replaced -- so a retry is safe.
    """
    import time

    from requests.exceptions import ConnectionError as RequestsConnectionError

    for attempt in range(1, attempts + 1):
        try:
            upload_table(table)
            return
        except (RequestsConnectionError, OSError) as exc:
            if attempt == attempts:
                raise
            wait = 10 * attempt
            print(
                f"[{table}] {type(exc).__name__} on attempt {attempt}; retrying in {wait}s"
            )
            time.sleep(wait)


def main() -> int:
    tables = sys.argv[1:] or [t for t in TABLES if (output_dir() / t).exists()]
    unknown = [t for t in tables if t not in TABLES]
    if unknown:
        raise SystemExit(f"unknown tables: {unknown}")
    for table in tables:
        upload_with_retry(table)
    print(
        f"\nuploaded {len(tables)} table(s) to {BILLING_PROJECT}.{DATASET_ID}_staging"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
