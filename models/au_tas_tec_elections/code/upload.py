"""Upload cleaned au_tas_tec_elections parquet to BigQuery staging.

Usage:
    PYTHONPATH=. ~/.venvs/bd-pipelines-tas/bin/python models/au_tas_tec_elections/code/upload.py \
        [--env dev] [table_slug ...]

Reads the cleaned parquet from ``$TEC_DATA_ROOT/output`` (default
``~/Downloads/au_tas_tec_elections_data/output``), which is never under the repo or Dropbox.
Uploads smallest table first and stops on the first failure.

Prod table data is materialised by the table-approve action on merge, not by running
this against prod. See ``.claude/rules/onboarding-workflow.md``.
"""

from __future__ import annotations

import inspect
import json
import sys
import warnings

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
from google.cloud import bigquery  # noqa: E402

from pipelines.datasets.au_tas_tec_elections.constants import (  # noqa: E402
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

# Measured row counts, written by the cleaning step as a flat {slug: rows} map. A count
# that is present is asserted after upload so a partial load cannot pass silently; a
# table absent from the file is uploaded without an assertion.
ROW_COUNTS_PATH = OUTPUT_ROOT / "row_counts.json"
EXPECTED_ROWS: dict[str, int] = (
    json.loads(ROW_COUNTS_PATH.read_text(encoding="utf-8"))
    if ROW_COUNTS_PATH.exists()
    else {}
)

# Smallest table first, so a schema or credential problem surfaces on a cheap upload.
# Tables with no measured count sort last, keeping publication order among themselves.
TABLES = sorted(
    constants.TABLES.value,
    key=lambda slug: EXPECTED_ROWS.get(slug, float("inf")),
)


def upload_table(slug: str, expected_rows: int | None) -> int:
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
    if expected_rows is None:
        print(f"  {slug}: uploaded {rows:,} rows (no expected count on file)")
        return rows
    if rows != expected_rows:
        raise SystemExit(
            f"  {slug}: uploaded {rows:,} rows but expected {expected_rows:,} — aborting"
        )
    print(
        f"  {slug}: uploaded {rows:,} rows (expected {expected_rows:,}) — OK"
    )
    return rows


def main() -> None:
    targets = _argv or list(TABLES)
    print(f"env={ENV} billing={BILLING_PROJECT} dataset={DATASET_ID}")
    total = 0
    for slug in TABLES:
        if slug in targets:
            print(f"[upload] {slug}")
            total += upload_table(slug, EXPECTED_ROWS.get(slug))
    print(f"done. {total:,} rows uploaded.")


if __name__ == "__main__":
    main()
