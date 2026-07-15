"""Upload cleaned parquet tables of br_bd_diretorios_us to BigQuery dev.

Uploads each table to gs://basedosdados-dev staging and creates
basedosdados-dev.br_bd_diretorios_us_staging.<table_slug>, then verifies
row counts against expected values. Stops at the first failure.
"""

import sys
from pathlib import Path

import basedosdados as bd
import google.cloud.storage as gcs

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "br_bd_diretorios_us"
ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT / "data" / "output"

# Monkey-patch for requester-pays bucket
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

# (table_slug, expected_row_count) — upload in this order
TABLES = [
    ("state", 60),
    ("county", 3236),
    ("place", 32350),
    ("census_tract_2020", 85396),
    ("zcta_2020", 33791),
    ("cbsa_2023", 937),
    ("congressional_district_119", 440),
    ("puma_2020", 2487),
    ("school_district", 19637),
    ("school", 102274),
    ("higher_education_institution", 6072),
    ("congress_member", 12767),
    ("naics_2022", 2125),
    ("soc_2018", 1447),
    ("census_industry_2002", 270),
    ("census_industry_2007", 270),
    ("census_industry_2012", 269),
    ("census_industry_2017", 271),
    ("census_occupation_2002", 510),
    ("census_occupation_2010", 540),
    ("census_occupation_2018", 570),
]


def upload_table(table_slug: str, expected_rows: int) -> int:
    path = OUTPUT_DIR / table_slug / "data.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing parquet file: {path}")

    # Delete stale GCS staging prefix before upload
    st = bd.Storage(dataset_id=DATASET_ID, table_id=table_slug)
    st.delete_table(mode="staging", not_found_ok=True)

    tb = bd.Table(dataset_id=DATASET_ID, table_id=table_slug)
    tb.create(
        path=path,
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    # Verify row count in BigQuery staging table
    query = (
        f"select count(*) as n from "
        f"`{BILLING_PROJECT}.{DATASET_ID}_staging.{table_slug}`"
    )
    df = bd.read_sql(query, billing_project_id=BILLING_PROJECT, from_file=True)
    n = int(df["n"].iloc[0])
    match = "MATCH" if n == expected_rows else "MISMATCH"
    print(
        f"[{table_slug}] uploaded — rows={n} expected={expected_rows} {match}"
    )
    if n != expected_rows:
        raise ValueError(
            f"Row count mismatch for {table_slug}: got {n}, expected {expected_rows}"
        )
    return n


def main() -> None:
    only = sys.argv[1:] or None
    for table_slug, expected in TABLES:
        if only and table_slug not in only:
            continue
        try:
            upload_table(table_slug, expected)
        except Exception as e:
            print(f"[{table_slug}] FAILED — {e}")
            sys.exit(1)
    print("All uploads complete.")


if __name__ == "__main__":
    main()
