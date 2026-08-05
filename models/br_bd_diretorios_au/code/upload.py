"""Upload cleaned Parquet tables of br_bd_diretorios_au to BigQuery dev.

Uploads each table to gs://basedosdados-dev staging and creates
basedosdados-dev.br_bd_diretorios_au_staging.<table_slug>, then verifies row
counts against expected values. Stops at the first failure.

Run:  python upload.py            # all tables
      python upload.py sa2_2021   # a subset
"""

import sys
from pathlib import Path

import basedosdados as bd
import google.cloud.storage as gcs

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "br_bd_diretorios_au"
ROOT = Path(__file__).resolve().parent.parent  # models/br_bd_diretorios_au
OUTPUT_DIR = ROOT / "output"

# Monkey-patch for requester-pays bucket
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

# (table_slug, expected_row_count) — verified against ABS allocation files
TABLES = [
    ("state", 10),
    # 2021 (ASGS Edition 3)
    ("sa1_2021", 61845),
    ("sa2_2021", 2473),
    ("sa3_2021", 359),
    ("sa4_2021", 108),
    ("gccsa_2021", 35),
    ("lga_2021", 566),
    ("postal_area_2021", 2644),
    ("suburb_2021", 15353),
    ("commonwealth_electoral_division_2021", 170),
    ("state_electoral_division_2021", 452),
    # 2016 (ASGS Edition 2)
    ("sa1_2016", 57523),
    ("sa2_2016", 2310),
    ("sa3_2016", 358),
    ("sa4_2016", 107),
    ("gccsa_2016", 34),
    ("lga_2016", 563),
    ("postal_area_2016", 2670),
    ("suburb_2016", 15304),
    ("commonwealth_electoral_division_2016", 168),
    ("state_electoral_division_2016", 448),
    # correspondences (crosswalks)
    ("correspondence_sa2_2016_2021", 2501),  # 2504 minus 3 null-endpoint rows
    ("correspondence_lga_2016_2021", 582),  # 585 minus 3 null-endpoint rows
]


def upload_table(table_slug: str, expected_rows: int) -> int:
    path = OUTPUT_DIR / f"{table_slug}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing parquet file: {path}")

    # Delete stale GCS staging prefix before upload
    st = bd.Storage(dataset_id=DATASET_ID, table_id=table_slug)
    st.delete_table(mode="staging", not_found_ok=True)

    tb = bd.Table(dataset_id=DATASET_ID, table_id=table_slug)
    tb.create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

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
