"""Upload cleaned au_vic_vec_elections parquet to BigQuery staging.

Usage:
    PYTHONPATH=. ~/.venvs/bd-pipelines-vic/bin/python models/au_vic_vec_elections/code/upload.py \
        [--env dev] [table_slug ...]

Reads the cleaned parquet from ``$VEC_DATA_ROOT/output`` (default
``~/Downloads/au_vic_vec_elections_data/output``), which is never under the repo or Dropbox.
Uploads smallest table first and stops on the first failure.

Prod table data is materialised by the table-approve action on merge, not by running
this against prod. See ``.claude/rules/onboarding-workflow.md``.
"""

from __future__ import annotations

import inspect
import sys
import warnings

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
from google.cloud import bigquery  # noqa: E402

from pipelines.datasets.au_vic_vec_elections.constants import (  # noqa: E402
    constants,
    data_root,
)

_argv = sys.argv[1:]
if "--env" in _argv:
    _i = _argv.index("--env")
    ENV = _argv[_i + 1]
    _argv = _argv[:_i] + _argv[_i + 2 :]
else:
    ENV = "dev"

BILLING_PROJECT = "basedosdados" if ENV == "prod" else "basedosdados-dev"
DATASET_ID = constants.DATASET_ID.value
OUTPUT_ROOT = data_root() / "output"

_orig_bucket = gcs.Client.bucket
_BUCKET_TAKES_GENERATION = (
    "generation" in inspect.signature(_orig_bucket).parameters
)


def _patched_bucket(
    self: gcs.Client,
    bucket_name: str,
    user_project: str | None = None,
    generation: int | None = None,
) -> gcs.Bucket:
    """Force ``user_project`` so the requester-pays bucket bills our project."""
    kwargs: dict[str, object] = {"user_project": BILLING_PROJECT}
    if _BUCKET_TAKES_GENERATION and generation is not None:
        kwargs["generation"] = generation
    return _orig_bucket(self, bucket_name, **kwargs)


gcs.Client.bucket = _patched_bucket

# (table_slug, expected_rows) — smallest first. The counts are measured, and are
# asserted after upload so a partial load cannot pass silently.
TABLES = [
    ("election", 12),
    ("dicionario", 64),
    ("enrolment_turnout", 483),
    ("candidate", 4_412),
    ("disclosure_gift", 4_374),
    ("result_district", 5_963),
    ("distribution_of_preferences", 115_661),
    ("result_voting_centre", 267_023),
]


def upload_table(slug: str, expected_rows: int) -> int:
    path = OUTPUT_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"Missing output path: {path}")

    tb = bd.Table(dataset_id=DATASET_ID, table_id=slug)
    st = bd.Storage(dataset_id=DATASET_ID, table_id=slug)
    try:
        st.delete_table(mode="staging", not_found_ok=True)
    except Exception as exc:
        print(f"  [warn] staging prefix cleanup: {exc}")

    tb.create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    client = bigquery.Client(project=BILLING_PROJECT)
    query = f"select count(*) as n from `{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}`"
    rows = next(iter(client.query(query).result())).n
    if rows != expected_rows:
        raise SystemExit(
            f"  {slug}: uploaded {rows:,} rows but expected {expected_rows:,} — aborting"
        )
    print(
        f"  {slug}: uploaded {rows:,} rows (expected {expected_rows:,}) — OK"
    )
    return rows


def main() -> None:
    targets = _argv or [t[0] for t in TABLES]
    print(f"env={ENV} billing={BILLING_PROJECT} dataset={DATASET_ID}")
    total = 0
    for slug, expected in TABLES:
        if slug in targets:
            print(f"[upload] {slug}")
            total += upload_table(slug, expected)
    print(f"done. {total:,} rows uploaded.")


if __name__ == "__main__":
    main()
