"""Upload cleaned af_afrobarometer_survey Parquet tables to BigQuery.

Uploads output/<slug>/data.parquet into <project>_staging.<slug>.

Usage:
    .venv/bin/python models/af_afrobarometer_survey/code/upload.py [--prod] [slug ...]

Default project is basedosdados-dev. Uploads smallest first; stops on the
first failure; verifies row counts against the cleaned Parquet.
"""

import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
from google.cloud import bigquery  # noqa: E402

DATASET_ID = "af_afrobarometer_survey"
OUTPUT_ROOT = Path(__file__).resolve().parent.parent / "output"

# (slug, expected_rows) — smallest first
TABLES = [
    ("round1", 21_531),
    ("round2", 24_301),
    ("round3", 25_397),
    ("round4", 27_713),
    ("round7", 45_823),
    ("round8", 48_084),
    ("dicionario", 48_675),
    ("round5", 51_587),
    ("round9", 53_444),
    ("round6", 53_935),
]


def make_bucket_patch(billing_project):
    orig = gcs.Client.bucket

    def patched(self, bucket_name, user_project=None):
        return orig(self, bucket_name, user_project=billing_project)

    gcs.Client.bucket = patched


def upload_table(slug: str, expected_rows: int, billing_project: str) -> int:
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

    client = bigquery.Client(project=billing_project)
    q = (
        f"select count(*) as n from "
        f"`{billing_project}.{DATASET_ID}_staging.{slug}`"
    )
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
    args = sys.argv[1:]
    prod = "--prod" in args
    only = set(a for a in args if a != "--prod")
    billing_project = "basedosdados" if prod else "basedosdados-dev"
    make_bucket_patch(billing_project)
    print(f"[upload] project={billing_project}")
    tables = [(s, r) for s, r in TABLES if not only or s in only]
    for slug, expected in tables:
        print(f"=== {slug} ===", flush=True)
        try:
            upload_table(slug, expected, billing_project)
        except Exception as e:
            print(f"  FAILED: {type(e).__name__}: {e}")
            sys.exit(1)
    print("ALL TABLES UPLOADED")


if __name__ == "__main__":
    main()
