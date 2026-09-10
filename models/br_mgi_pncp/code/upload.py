"""Upload cleaned br_mgi_pncp parquet to BigQuery dev staging.

Usage:
    uv run python models/br_mgi_pncp/code/upload.py [--table <slug>]

Dev only: billing and target project is basedosdados-dev. Prod table data is
materialised by the table-approve action when the onboarding PR merges, never
uploaded from here.

The parquet is all-STRING by design (see ``utils.py``); the dbt model does the
``safe_cast``. Expected row counts come from ``output/clean_summary.json``, so
the upload asserts against what the cleaning step actually produced rather than
a number typed in by hand.

Those counts are *pre-deduplication*: staging holds one row per record per
harvest window that touched it, and the dbt models collapse them on the PNCP
control number. The materialized tables will therefore be smaller than staging,
which is expected, not a loss.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
from google.cloud import bigquery  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.br_mgi_pncp.constants import constants  # noqa: E402

BILLING_PROJECT = "basedosdados-dev"  # DEV ONLY — never prod
DATASET_ID = "br_mgi_pncp"
DATA_ROOT = Path(
    os.environ.get(
        "PNCP_DATA_DIR", Path.home() / "Downloads" / "br_mgi_pncp_data"
    )
)
OUTPUT_ROOT = DATA_ROOT / "output"

# Smallest first, so a credentials or convention problem surfaces cheaply.
# Derived from constants rather than hardcoded: a deferred table (see
# constants.DEFERRED_TABLES) has no cleaned output, and uploading it would
# create an empty staging table that the dbt model then reads.
#
# The dicionario is NOT uploaded: it is a dbt model derived from the fact
# models (see gen_dbt.dicionario_sql), so it has no staging table.
_PREFERRED = [
    "instrumento_cobranca",
    "ata_registro_preco",
    "contratacao",
    "contrato",
]
_SCOPED = set(constants.FACT_TABLES.value)
TABLE_ORDER = [t for t in _PREFERRED if t in _SCOPED] + [
    t for t in constants.FACT_TABLES.value if t not in _PREFERRED
]

# Requester-pays bucket: force user_project onto every bucket handle.
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None, **kwargs):
    return _orig_bucket(
        self, bucket_name, user_project=BILLING_PROJECT, **kwargs
    )


gcs.Client.bucket = _patched_bucket


def expected_rows() -> dict[str, int]:
    summary_path = OUTPUT_ROOT / "clean_summary.json"
    if not summary_path.exists():
        raise SystemExit(f"missing {summary_path}; run clean.py first")
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    return {s["table"]: s["written_rows"] for s in summary}


def upload_table(slug: str, expected: int) -> int:
    path = OUTPUT_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"missing output path: {path}")

    storage = bd.Storage(dataset_id=DATASET_ID, table_id=slug)
    try:
        storage.delete_table(mode="staging", not_found_ok=True)
    except Exception as exc:
        print(f"  [warn] staging prefix cleanup: {exc}")

    bd.Table(dataset_id=DATASET_ID, table_id=slug).create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    client = bigquery.Client(project=BILLING_PROJECT)
    fqn = f"{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}"
    n = next(
        iter(client.query(f"select count(*) as n from `{fqn}`").result())
    ).n
    print(
        f"  {slug}: {n:,} rows (expected {expected:,}) — {'OK' if n == expected else 'MISMATCH'}"
    )
    print(f"  staging: {fqn}")
    if n != expected:
        raise SystemExit(
            f"  {slug}: row count mismatch — aborting before the next table"
        )
    return n


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--table", default=None, choices=TABLE_ORDER)
    args = ap.parse_args()

    counts = expected_rows()
    targets = [args.table] if args.table else TABLE_ORDER
    print(f"env=dev billing={BILLING_PROJECT} dataset={DATASET_ID}")

    results = {}
    for slug in targets:
        if slug not in counts:
            print(f"[skip] {slug}: not present in clean_summary.json")
            continue
        print(f"[upload] {slug}")
        results[slug] = upload_table(slug, counts[slug])

    print(f"\n=== UPLOAD COMPLETE: {DATASET_ID} (env=dev) ===")
    for slug, n in results.items():
        print(f"  OK {slug}: {n:,} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
