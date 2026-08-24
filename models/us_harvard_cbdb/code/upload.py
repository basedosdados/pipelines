"""Upload us_harvard_cbdb typed parquet to BigQuery dev (basedosdados-dev).

Unpartitioned tables: output/<table>/data.parquet -> staging.<table>.
Usage: uv run python upload.py [table1 table2 ...]   (default: all in order)
"""

import os
import sys

os.environ.setdefault(
    "GOOGLE_APPLICATION_CREDENTIALS",
    os.path.expanduser("~/.basedosdados/credentials/staging.json"),
)

import basedosdados as bd
from google.cloud import storage as _gcs

# pyrefly: ignore [missing-import]
from schema_spec import TABLE_ORDER

BILLING = "basedosdados-dev"
DATASET = "us_harvard_cbdb"
OUT = os.path.expanduser("~/Downloads/us_harvard_cbdb_data/output")

# requester-pays bucket: force user_project on every bucket() call
_orig_bucket = _gcs.Client.bucket


def _bucket(self, name, user_project=BILLING):
    return _orig_bucket(self, name, user_project=user_project)


# pyrefly: ignore [bad-assignment]
_gcs.Client.bucket = _bucket


def upload_one(name):
    path = os.path.join(OUT, name, "data.parquet")
    assert os.path.exists(path), f"missing {path}"
    t = bd.Table(dataset_id=DATASET, table_id=name)
    t.create(
        path=path,
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )
    print(f"  OK  {name}")


def main():
    tables = sys.argv[1:] or TABLE_ORDER
    for name in tables:
        print(f"[upload] {name} ...")
        upload_one(name)
    print("ALL DONE:", ", ".join(tables))


if __name__ == "__main__":
    main()
