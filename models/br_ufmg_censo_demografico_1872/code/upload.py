"""Upload the cleaned br_ufmg_censo_demografico_1872 Parquet tables to BigQuery.

Usage:
    uv run python models/br_ufmg_censo_demografico_1872/code/upload.py \
        [--env dev|prod] [table_slug ...]

--env dev (default) -> basedosdados-dev; --env prod -> basedosdados. Point
GOOGLE_APPLICATION_CREDENTIALS at the matching service account. Uploads
smallest first and stops on the first failure.

Expected row counts are read from the Parquet on disk rather than hardcoded,
so the check catches an upload that dropped rows without needing a table of
magic numbers to be kept in sync with the transform.
"""

import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from google.cloud import bigquery  # noqa: E402

from models.br_ufmg_censo_demografico_1872.code.spec import (  # noqa: E402
    LEVELS,
    table_slug,
)
from models.br_ufmg_censo_demografico_1872.code.spec import (  # noqa: E402
    TABLES as SOURCE_TABLES,
)
from models.br_ufmg_censo_demografico_1872.code.tables import (  # noqa: E402
    AUXILIARES,
    DATASET_ID,
)

_argv = sys.argv[1:]
if "--env" in _argv:
    _i = _argv.index("--env")
    ENV = _argv[_i + 1]
    _argv = _argv[:_i] + _argv[_i + 2 :]
else:
    ENV = "dev"

BILLING_PROJECT = "basedosdados" if ENV == "prod" else "basedosdados-dev"

import os  # noqa: E402

DATA_DIR = Path(
    os.environ.get(
        "CENSO_1872_DATA",
        Path.home() / "Downloads" / "br_ufmg_censo_demografico_1872_data",
    )
)
OUTPUT_ROOT = DATA_DIR / "output"

# The staging bucket is requester-pays, so every bucket handle needs a billing
# project attached.
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket


def all_slugs() -> list[str]:
    slugs = list(AUXILIARES)
    for source in SOURCE_TABLES:
        for level in LEVELS:
            slugs.append(table_slug(source, level))
    return slugs


def expected_rows(slug: str) -> int:
    files = sorted((OUTPUT_ROOT / slug).glob("ano=*/data.parquet"))
    if not files:
        raise FileNotFoundError(f"no parquet under {OUTPUT_ROOT / slug}")
    return sum(pq.read_metadata(f).num_rows for f in files)


def upload_table(slug: str, expected: int) -> int:
    path = OUTPUT_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"missing output path: {path}")

    # Clear the stale staging prefix first: leftover files under a different
    # partition layout make BigQuery reject the external table.
    st = bd.Storage(dataset_id=DATASET_ID, table_id=slug)
    try:
        st.delete_table(mode="staging", not_found_ok=True)
    except Exception as e:
        print(f"  [warn] staging prefix cleanup: {e}")

    tb = bd.Table(dataset_id=DATASET_ID, table_id=slug)
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
    if n != expected:
        raise ValueError(
            f"{slug}: staging has {n:,} rows, expected {expected:,}"
        )
    print(f"  {slug}: {n:,} rows OK", flush=True)
    return n


def main() -> None:
    only = set(_argv)
    slugs = [s for s in all_slugs() if not only or s in only]
    slugs.sort(key=expected_rows)

    print(
        f"=== uploading {len(slugs)} tables to {BILLING_PROJECT} (env={ENV}) ===",
        flush=True,
    )
    for i, slug in enumerate(slugs, 1):
        print(f"[{i}/{len(slugs)}] {slug}", flush=True)
        try:
            upload_table(slug, expected_rows(slug))
        except Exception as e:
            print(f"  FAILED: {type(e).__name__}: {e}")
            sys.exit(1)
    print("ALL TABLES UPLOADED")


if __name__ == "__main__":
    main()
