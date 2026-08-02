"""Upload cleaned us_bls_qcew parquet tables to BigQuery.

Usage:
    uv run python models/us_bls_qcew/code/upload.py [--env dev|prod] [table ...]

--env dev (default) -> basedosdados-dev. Point GOOGLE_APPLICATION_CREDENTIALS at
the matching service account. Uploads sequentially and stops on first failure.
Row counts are reported (not asserted against a fixed expectation, since the dev
subset is partial); the printed counts are the verification artifact.
"""

import os
import subprocess
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
from google.cloud import bigquery  # noqa: E402


def _gcloud_project():
    """The active gcloud project (a billing project the local ADC can use)."""
    try:
        return (
            subprocess.check_output(
                ["gcloud", "config", "get-value", "project"],
                text=True,
                stderr=subprocess.DEVNULL,
            ).strip()
            or None
        )
    except Exception:
        return None


_argv = sys.argv[1:]
if "--env" in _argv:
    _i = _argv.index("--env")
    ENV = _argv[_i + 1]
    _argv = _argv[:_i] + _argv[_i + 2 :]
else:
    ENV = "dev"
BILLING_PROJECT = "basedosdados" if ENV == "prod" else "basedosdados-dev"
DATASET_ID = "us_bls_qcew"
OUTPUT_ROOT = Path(__file__).resolve().parent.parent / "output"

_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket


def upload_table(slug: str) -> int:
    path = OUTPUT_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"Missing output path: {path}")

    tb = bd.Table(dataset_id=DATASET_ID, table_id=slug)
    st = bd.Storage(dataset_id=DATASET_ID, table_id=slug)
    try:
        st.delete_table(mode="staging", not_found_ok=True)
    except Exception as e:
        print(f"  [warn] staging prefix cleanup: {e}")

    tb.create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    # Verify row count. The local ADC identity can create the staging table but
    # may lack bigquery.jobs.create in basedosdados-dev; bill the count job to a
    # project where it can (BQ_BILLING env, default the gcloud project), reading
    # the dev table cross-project. Non-fatal: the create above is the real work.
    q = f"select count(*) as n from `{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}`"
    for job_project in [
        BILLING_PROJECT,
        os.environ.get("BQ_BILLING"),
        _gcloud_project(),
    ]:
        if not job_project:
            continue
        try:
            n = next(
                iter(bigquery.Client(project=job_project).query(q).result())
            ).n
            print(
                f"  {slug}: uploaded, {n:,} rows (verified via {job_project})",
                flush=True,
            )
            return n
        except Exception as e:
            last = e
    print(
        f"  {slug}: uploaded (staging table created); count not verified locally: {last}",
        flush=True,
    )
    return -1


def all_slugs() -> list[str]:
    # Skip bookkeeping dirs like ``.done`` (per-year resumability markers written
    # by a --full clean); only real table directories are uploaded.
    return sorted(
        p.name
        for p in OUTPUT_ROOT.iterdir()
        if p.is_dir() and not p.name.startswith(".")
    )


def main():
    only = set(_argv)
    slugs = [s for s in all_slugs() if not only or s in only]
    if not slugs:
        print("no output tables found; run clean_data.py first")
        sys.exit(1)
    print(
        f"=== uploading {len(slugs)} tables to {BILLING_PROJECT} (env={ENV}) ===",
        flush=True,
    )
    total = 0
    for slug in slugs:
        print(f"=== {slug} ===", flush=True)
        try:
            n = upload_table(slug)
            total += max(n, 0)
        except Exception as e:
            print(f"  FAILED: {type(e).__name__}: {e}")
            sys.exit(1)
    print(f"ALL TABLES UPLOADED — {total:,} rows verified")


if __name__ == "__main__":
    main()
