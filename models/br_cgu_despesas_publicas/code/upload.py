"""Upload the cleaned br_cgu_despesas_publicas parquet to BigQuery.

Usage:
    uv run python models/br_cgu_despesas_publicas/code/upload.py \
        [--env dev|prod] [--table execucao|favorecido]

``--env dev`` (default) targets ``basedosdados-dev``. Prod is never uploaded from
a laptop — the prod table is materialised by the table-approve action on merge —
so the prod branch exists only for completeness and should not normally be used.

The expected row count is read from ``clean_summary.json`` written by clean.py,
so the check is against what was actually cleaned rather than a hardcoded number.
"""

import json
import os
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
from google.cloud import bigquery  # noqa: E402

_argv = sys.argv[1:]
ENV = "dev"
if "--env" in _argv:
    _i = _argv.index("--env")
    ENV = _argv[_i + 1]

BILLING_PROJECT = "basedosdados" if ENV == "prod" else "basedosdados-dev"
DATASET_ID = "br_cgu_despesas_publicas"

if "--table" in _argv:
    TABLE_ID = _argv[_argv.index("--table") + 1]
else:
    TABLE_ID = "execucao"

# Each table keeps its own scratch tree; override with the matching env var.
_DEFAULT_DATA = {
    "execucao": "br_cgu_despesas_publicas_data",
    "favorecido": "br_cgu_favorecidos_data",
}[TABLE_ID]
DATA = Path(
    os.environ.get(
        "BR_CGU_DESPESAS_DATA",
        Path.home() / "Downloads" / _DEFAULT_DATA,
    )
)

# The GCS bucket is requester-pays.
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket


def main() -> None:
    path = DATA / "output" / TABLE_ID
    if not path.exists():
        raise SystemExit(f"Missing cleaned output: {path}")
    expected = json.loads((DATA / "clean_summary.json").read_text())[
        "total_rows"
    ]

    print(
        f"=== uploading {TABLE_ID} to {BILLING_PROJECT} (env={ENV}) ===",
        flush=True,
    )

    # Delete the stale staging prefix first: leftover objects from an earlier
    # layout are merged into the external table and break it on a schema change.
    st = bd.Storage(dataset_id=DATASET_ID, table_id=TABLE_ID)
    try:
        st.delete_table(mode="staging", not_found_ok=True)
    except Exception as e:
        print(f"  [warn] staging prefix cleanup: {e}")

    tb = bd.Table(dataset_id=DATASET_ID, table_id=TABLE_ID)
    tb.create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    client = bigquery.Client(project=BILLING_PROJECT)
    q = f"select count(*) as n from `{BILLING_PROJECT}.{DATASET_ID}_staging.{TABLE_ID}`"
    n = next(iter(client.query(q).result())).n
    print(f"  uploaded {n:,} rows (expected {expected:,})")
    if n != expected:
        raise SystemExit(f"ROW MISMATCH: {n:,} != {expected:,}")
    print("UPLOAD OK")


if __name__ == "__main__":
    main()
