"""Upload cleaned Parquet tables of au_abs_community_profiles to BigQuery dev.

Uploads every output/packs/<slug>.parquet to
basedosdados-dev.au_abs_community_profiles_staging.<slug> and reports row counts.
One-shot onboarding upload -> typed parquet is fine (this is NOT the recurring
pipeline path; the dbt model safe_casts every column).

Run:  python upload.py            # all present parquet tables
      python upload.py state sa3  # a subset
"""

import glob
import sys
from pathlib import Path

import basedosdados as bd
import google.cloud.storage as gcs

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "au_abs_community_profiles"
ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT / "output" / "packs"

# requester-pays bucket
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket


def upload_table(slug: str) -> int:
    path = OUTPUT_DIR / f"{slug}.parquet"
    if not path.exists():
        raise FileNotFoundError(path)
    st = bd.Storage(dataset_id=DATASET_ID, table_id=slug)
    st.delete_table(mode="staging", not_found_ok=True)
    tb = bd.Table(dataset_id=DATASET_ID, table_id=slug)
    tb.create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )
    q = f"select count(*) as n from `{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}`"
    n = int(
        bd.read_sql(q, billing_project_id=BILLING_PROJECT, from_file=True)[
            "n"
        ].iloc[0]
    )
    print(f"[{slug}] uploaded — rows={n:,}")
    return n


def main() -> None:
    only = set(a for a in sys.argv[1:]) or None
    slugs = [
        Path(p).stem for p in sorted(glob.glob(str(OUTPUT_DIR / "*.parquet")))
    ]
    for slug in slugs:
        if only and slug not in only:
            continue
        try:
            upload_table(slug)
        except Exception as e:
            print(f"[{slug}] FAILED — {e}")
            sys.exit(1)
    print("All uploads complete.")


if __name__ == "__main__":
    main()
