"""Upload cleaned au_rba_statistical_tables parquet to BigQuery staging.

Usage:
    uv run python models/au_rba_statistical_tables/code/upload.py [--env dev|prod] [table_slug ...]

--env dev (default) -> basedosdados-dev. Point GOOGLE_APPLICATION_CREDENTIALS at
the matching service account. Reads the cleaned parquet from $RBA_DATA/output
(default ~/Downloads/au_rba_statistical_tables_data/output), which is never under
the repo or Dropbox. Uploads smallest table first and stops on first failure.

Note: prod table data is materialised by the table-approve action on merge, not
by running this with --env prod. See .claude/rules/onboarding-workflow.md.
"""

import inspect
import os
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
from google.cloud import bigquery  # noqa: E402

_argv = sys.argv[1:]
if "--env" in _argv:
    _i = _argv.index("--env")
    ENV = _argv[_i + 1]
    _argv = _argv[:_i] + _argv[_i + 2 :]
else:
    ENV = "dev"

BILLING_PROJECT = "basedosdados" if ENV == "prod" else "basedosdados-dev"
DATASET_ID = "au_rba_statistical_tables"
DATA_ROOT = Path(
    os.environ.get(
        "RBA_DATA",
        Path.home() / "Downloads" / "au_rba_statistical_tables_data",
    )
)
OUTPUT_ROOT = DATA_ROOT / "output"

_orig_bucket = gcs.Client.bucket
# `generation` was added to Client.bucket in a later google-cloud-storage; only
# forward it when the installed version actually accepts it.
_BUCKET_TAKES_GENERATION = (
    "generation" in inspect.signature(_orig_bucket).parameters
)


def _patched_bucket(
    self: gcs.Client,
    bucket_name: str,
    user_project: str | None = None,
    generation: int | None = None,
) -> "gcs.Bucket":
    """Force ``user_project`` so the requester-pays bucket bills our project."""
    kwargs: dict[str, object] = {"user_project": BILLING_PROJECT}
    if _BUCKET_TAKES_GENERATION and generation is not None:
        kwargs["generation"] = generation
    return _orig_bucket(self, bucket_name, **kwargs)


gcs.Client.bucket = _patched_bucket

# (table_slug, expected_rows) — smallest first
TABLES = [
    ("dicionario", 5),
    ("series_break", 2_034),
    ("series", 3_861),
    ("observation", 924_206),
]


def upload_table(slug: str, expected_rows: int) -> int:
    """Upload one cleaned parquet table to BigQuery staging and verify the count."""
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

    client = bigquery.Client(project=BILLING_PROJECT)
    q = f"select count(*) as n from `{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}`"
    n = next(iter(client.query(q).result())).n
    if n != expected_rows:
        raise SystemExit(
            f"  {slug}: uploaded {n:,} rows but expected {expected_rows:,} — aborting"
        )
    print(f"  {slug}: uploaded {n:,} rows (expected {expected_rows:,}) — OK")
    return n


def main() -> None:
    """Upload each table in ``TABLES`` (smallest first), stopping on failure."""
    targets = _argv or [t[0] for t in TABLES]
    print(f"env={ENV} billing={BILLING_PROJECT} dataset={DATASET_ID}")
    for slug, expected in TABLES:
        if slug in targets:
            print(f"[upload] {slug}")
            upload_table(slug, expected)
    print("done.")


if __name__ == "__main__":
    main()
