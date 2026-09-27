"""One-shot upload of the cleaned parquet to the BigQuery dev staging dataset.

Uses the SAME helper the recurring flow uses (`pipelines.utils.tasks._upload_to_gcs`)
rather than `bd.Table.create` on the data. Two reasons, both load-bearing:

- `bd.Table.create(path=<parquet>)` reads the whole file into pandas and
  stringifies it, which balloons RAM on tables this size. The helper hands
  `dump_header` a 0-row header instead and streams the files, so RAM stays flat.
- A `load_table_from_uri` bootstrap would leave staging as a NATIVE table. The
  flow only ever writes parquet to GCS, and a native table never reads those
  files, so dbt would keep serving this bootstrap snapshot after every future
  release with nothing failing. Going through the same helper makes staging
  EXTERNAL over `gs://<bucket>/staging/<ds>/<table>/*`, which both paths share.

The script asserts `table_type == "EXTERNAL"` afterwards, because that is the only
visible tell that the two paths agree.

    python models/us_nchs_vital_statistics/code/us_nchs_vital_statistics_upload.py
    python .../us_nchs_vital_statistics_upload.py --tables birth
"""

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.us_nchs_vital_statistics.constants import (
    constants,
)
from pipelines.utils.tasks import _upload_to_gcs

DATASET_ID = constants.DATASET_ID.value
DATA = Path(
    os.environ.get(
        "NCHS_DATA_DIR",
        os.path.expanduser("~/Downloads/us_nchs_vital_statistics_data"),
    )
)
BUCKET = "basedosdados-dev"


def staging_table_type(table: str) -> str | None:
    from google.cloud import bigquery

    client = bigquery.Client(project=BUCKET)
    ref = f"{BUCKET}.{DATASET_ID}_staging.{table}"
    return client.get_table(ref).table_type


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tables", nargs="*", default=constants.TABLES.value)
    args = ap.parse_args()

    for table in args.tables:
        path = DATA / "output" / table
        if not path.exists():
            print(f"{table}: {path} missing, skipped")
            continue
        parts = sorted(path.glob("**/*.parquet"))
        size = sum(p.stat().st_size for p in parts)
        print(f"{table}: uploading {len(parts)} file(s), {size / 1e9:.2f} GB")
        _upload_to_gcs(
            data_path=str(path),
            dataset_id=DATASET_ID,
            table_id=table,
            bucket_name=BUCKET,
            dump_mode="append",
            source_format="parquet",
        )
        kind = staging_table_type(table)
        print(f"{table}: staging table_type = {kind}")
        if kind != "EXTERNAL":
            raise SystemExit(
                f"{table}: staging is {kind}, expected EXTERNAL. A native table "
                "ignores the GCS files the recurring flow writes, so every later "
                "release would be silently dropped. Drop it and re-run."
            )


if __name__ == "__main__":
    main()
