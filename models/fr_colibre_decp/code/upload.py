"""Upload the cleaned fr_colibre_decp parquet to BigQuery dev staging.

Usage (from the repo root):
    python -m models.fr_colibre_decp.code.upload [table ...]

This machine's config.toml is provisioned for basedosdados-dev only. Prod table
data is materialised by the table-approve action when the PR merges.

It goes through ``pipelines.datasets.fr_colibre_decp.tasks.replace_staging``,
the same function the recurring pipeline uses, so both paths write identical
all-STRING staging files.
"""

import sys
import warnings

import google.cloud.storage as gcs
import pyarrow.parquet as pq
from google.cloud import bigquery

from pipelines.datasets.fr_colibre_decp.constants import constants
from pipelines.datasets.fr_colibre_decp.tasks import replace_staging

warnings.filterwarnings("ignore")

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = constants.DATASET_ID.value


def _patch_requester_pays() -> None:
    original = gcs.Client.bucket

    def patched(self, bucket_name, user_project=None):
        return original(self, bucket_name, user_project=BILLING_PROJECT)

    gcs.Client.bucket = patched


def main() -> int:
    _patch_requester_pays()
    output = constants.DATA_DIR.value / "output"
    client = bigquery.Client(project=BILLING_PROJECT)
    for table in sys.argv[1:] or constants.TABLES.value:
        expected = sum(
            pq.ParquetFile(p).metadata.num_rows
            for p in (output / table).glob("ano=*/*.parquet")
        )
        print(f"=== {table}: {expected:,} rows on disk ===", flush=True)
        replace_staging(output / table, table, BILLING_PROJECT)
        query = f"select count(*) n from `{BILLING_PROJECT}.{DATASET_ID}_staging.{table}`"
        actual = next(iter(client.query(query).result())).n
        if actual != expected:
            print(f"  MISMATCH: staging has {actual:,}. Stopping.")
            return 1
        print(f"  OK: {actual:,} rows in staging", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
