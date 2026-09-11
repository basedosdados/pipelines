"""Upload the 12 cleaned Detailed-release tables to BigQuery (dev only).

Usage:
    uv run python models/au_abs_labour_force/code/upload_detailed.py [table ...]

**Uploads to ``basedosdados-dev`` and nowhere else, by design.** Data Basis never
writes prod from a workstation: the prod tables are materialised by the
table-approve action when the onboarding PR merges. An ``--env prod`` switch was
removed rather than fixed, because it could not have worked — ``bd.Table.create``
and ``bd.Storage`` take their destination project from ``~/.basedosdados/config.toml``,
not from anything this script sets, so the flag would have billed and verified
against prod while still writing dev, or written dev while reporting prod.

Parquet is read from ``<data-root>/output/<table>``, where ``<data-root>``
defaults to ``~/Downloads/au_abs_labour_force_data`` and is overridable with
``AU_ABS_LF_DATA`` — never from the repo or from Dropbox.

Uploads sequentially (smallest first), verifies the staged row count against the
expected shape, and stops on the first failure.
"""

import argparse
import os
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
from google.cloud import bigquery  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
from clean_detailed import EXPECTED_TABLE_ROWS  # noqa: E402

_parser = argparse.ArgumentParser(
    description="Upload the Detailed-release tables to basedosdados-dev."
)
_parser.add_argument(
    "tables",
    nargs="*",
    # The empty list is a valid choice on purpose: argparse validates the
    # nargs="*" default against `choices`, so omitting it makes "no table
    # arguments" (meaning all 12) an error. metavar keeps it out of --help.
    choices=[*sorted(EXPECTED_TABLE_ROWS), []],
    metavar="TABLE",
    help="tables to upload; default is all 12",
)
_argv = _parser.parse_args().tables

# Fixed, not selectable. See the module docstring: prod is materialised by
# table-approve on merge, and the SDK reads its destination from config.toml
# regardless of anything set here, so an environment switch could only lie.
BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "au_abs_labour_force"
DATA_ROOT = Path(
    os.environ.get(
        "AU_ABS_LF_DATA",
        Path.home() / "Downloads" / "au_abs_labour_force_data",
    )
).expanduser()
OUTPUT_ROOT = DATA_ROOT / "output"

# Monkey-patch for the requester-pays staging bucket.
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

# Smallest first, so a credentials or ACL problem surfaces cheaply.
TABLES = sorted(EXPECTED_TABLE_ROWS, key=lambda t: EXPECTED_TABLE_ROWS[t])


def upload_table(slug: str, expected_rows: int) -> int:
    path = OUTPUT_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"Missing output path: {path}")

    tb = bd.Table(dataset_id=DATASET_ID, table_id=slug)

    # Delete stale GCS staging prefix (avoids BQ partition key conflicts).
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

    status = "OK" if n == expected_rows else "ROW MISMATCH"
    print(
        f"  {slug}: uploaded {n:,} rows (expected {expected_rows:,}) — {status}"
    )
    if n != expected_rows:
        raise ValueError(
            f"{slug}: row count {n:,} != expected {expected_rows:,}"
        )
    return n


def main():
    only = set(_argv)
    unknown = only - set(TABLES)
    if unknown:
        raise SystemExit(f"unknown table(s): {sorted(unknown)}")
    tables = [t for t in TABLES if not only or t in only]
    print(
        f"=== uploading to {BILLING_PROJECT} from {OUTPUT_ROOT} ===",
        flush=True,
    )
    total = 0
    for slug in tables:
        print(f"=== {slug} ===", flush=True)
        try:
            total += upload_table(slug, EXPECTED_TABLE_ROWS[slug])
        except Exception as e:
            print(f"  FAILED: {type(e).__name__}: {e}")
            sys.exit(1)
    print(f"ALL {len(tables)} TABLES UPLOADED — {total:,} rows")


if __name__ == "__main__":
    main()
