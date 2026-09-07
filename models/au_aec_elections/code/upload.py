"""Upload cleaned au_aec_elections parquet to BigQuery staging.

Usage:
    uv run python models/au_aec_elections/code/upload.py [--env dev|prod] [table_slug ...]

--env dev (default) -> basedosdados-dev. Point GOOGLE_APPLICATION_CREDENTIALS at the
matching service account. Reads the cleaned parquet from $AEC_DATA/output (default
~/Downloads/au_aec_elections_data/output), which is never under the repo or Dropbox.
Uploads smallest table first and stops on the first failure.

Note: prod table data is materialised by the table-approve action on merge, not by
running this with --env prod. See .claude/rules/onboarding-workflow.md.
"""

from __future__ import annotations

import inspect
import sys
import warnings

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
from google.cloud import bigquery  # noqa: E402

from pipelines.datasets.au_aec_elections.constants import (  # noqa: E402
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
) -> gcs.Bucket:
    """Force ``user_project`` so the requester-pays bucket bills our project."""
    kwargs: dict[str, object] = {"user_project": BILLING_PROJECT}
    if _BUCKET_TAKES_GENERATION and generation is not None:
        kwargs["generation"] = generation
    return _orig_bucket(self, bucket_name, **kwargs)


gcs.Client.bucket = _patched_bucket

# (table_slug, expected_rows) — smallest first, as verified by validate.py
TABLES = [
    ("election", 34),
    ("dicionario", 85),
    ("party", 589),
    ("house_two_party_preferred_division", 1_202),
    ("division_summary", 2_570),
    ("senate_candidate", 3_492),
    ("referendum_polling_place", 8_735),
    ("house_candidate", 8_818),
    ("house_first_preference_division", 9_763),
    ("disclosure_election_return", 16_197),
    ("disclosure_return_annual", 20_977),
    ("house_two_party_preferred_polling_place", 70_576),
    ("disclosure_donation", 74_022),
    ("polling_place", 79_840),
    ("senate_first_preference_division", 123_442),
    ("disclosure_receipt", 124_282),
    ("house_two_candidate_preferred_polling_place", 141_152),
    ("house_first_preference_polling_place", 584_719),
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
