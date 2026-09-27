#!/usr/bin/env python3
"""Upload cleaned all-STRING parquet tables to BigQuery dev staging.

Generic over dataset: resolves each table's parquet (output/<name>.parquet file
or output/<name>/ dir), uploads to <dataset>_staging, and verifies the BQ row
count against the parquet metadata.

Usage (run from repo root, via uv):
    GOOGLE_APPLICATION_CREDENTIALS=~/.basedosdados/credentials/prod.json \
      uv run python models/us_nature_gerda/code/upload.py \
      --dataset us_nature_gerda --output models/us_nature_gerda/output [table ...]
"""

import argparse
import glob
import os
import sys
import warnings

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from google.cloud import bigquery  # noqa: E402

BILLING = "basedosdados-dev"

_orig = gcs.Client.bucket
gcs.Client.bucket = lambda self, name, user_project=None: _orig(
    self, name, user_project=BILLING
)


def resolve(output, name):
    f = os.path.join(output, f"{name}.parquet")
    if os.path.exists(f):
        return f, [f]
    d = os.path.join(output, name)
    if os.path.isdir(d):
        return d, glob.glob(os.path.join(d, "*.parquet"))
    raise FileNotFoundError(f"no parquet for {name} under {output}")


def parquet_rows(files):
    return sum(pq.read_metadata(f).num_rows for f in files)


def discover(output):
    names = [
        os.path.basename(p)[:-8]
        for p in glob.glob(os.path.join(output, "*.parquet"))
    ]
    names += [
        os.path.basename(d)
        for d in glob.glob(os.path.join(output, "*"))
        if os.path.isdir(d) and glob.glob(os.path.join(d, "*.parquet"))
    ]
    return sorted(set(names))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("tables", nargs="*")
    a = ap.parse_args()

    tables = a.tables or discover(a.output)
    client = bigquery.Client(project=BILLING)
    print(f"Uploading {len(tables)} table(s) to {a.dataset}_staging (dev)")
    for name in tables:
        path, files = resolve(a.output, name)
        expected = parquet_rows(files)
        print(f"=== {name} ({expected:,} rows) ===", flush=True)
        tb = bd.Table(dataset_id=a.dataset, table_id=name)
        st = bd.Storage(dataset_id=a.dataset, table_id=name)
        try:
            st.delete_table(mode="staging", not_found_ok=True)
        except Exception as e:
            print(f"  [warn] staging cleanup: {e}")
        tb.create(
            path=path,
            source_format="parquet",
            if_table_exists="replace",
            if_storage_data_exists="replace",
            if_dataset_exists="pass",
        )
        q = f"select count(*) n from `{BILLING}.{a.dataset}_staging.{name}`"
        n = next(iter(client.query(q).result())).n
        ok = "OK" if n == expected else "ROW MISMATCH"
        print(f"  uploaded {n:,} rows (expected {expected:,}) — {ok}")
        if n != expected:
            sys.exit(f"  FAILED: {name} row mismatch")
    print("ALL TABLES UPLOADED")


if __name__ == "__main__":
    main()
