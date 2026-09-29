#!/usr/bin/env python3
"""Upload the cleaned cl_ine_ene parquet to the dev staging bucket.

Uses `_upload_to_gcs` — the very helper the recurring flow's task wraps — rather
than `bd.Table.create(path=<data>)` or a BigQuery load job. That choice matters
twice over:

* `bd.Table.create` handed the full parquet reads all of it into pandas and
  stringifies it, which on 20M rows balloons to tens of GB. Through this helper
  it only ever sees a 0-row header file, so RAM stays flat and `Storage.upload`
  streams the partitions.
* A `load_table_from_uri` load job would leave staging as a NATIVE table. The
  flow writes only parquet to GCS, and a native table never reads those files —
  dbt would keep serving this bootstrap snapshot after every later release, with
  no error and no failing test. Going through the same helper as the flow leaves
  an EXTERNAL table over gs://<bucket>/staging/<ds>/<table>/, so both paths agree.

    python models/cl_ine_ene/code/upload.py --check
    python models/cl_ine_ene/code/upload.py

Run it with GOOGLE_APPLICATION_CREDENTIALS unset so the local ADC is used:

    env -u GOOGLE_APPLICATION_CREDENTIALS uv run --no-sync python .../upload.py
"""

from __future__ import annotations

import argparse
import os
import pathlib
import sys

REPO = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO))

DATA = pathlib.Path(
    os.environ.get(
        "CL_INE_ENE_DATA", pathlib.Path.home() / "Downloads/cl_ine_ene_data"
    )
)
DATASET_ID = "cl_ine_ene"
TABLE_ID = "microdato"
BUCKET = "basedosdados-dev"


def verify_local(path: pathlib.Path) -> int:
    """Refuse to upload a partition set that is not uniform and complete."""
    import pyarrow.parquet as pq

    files = sorted(path.glob("ano=*/mes=*/data.parquet"))
    if not files:
        raise SystemExit(
            f"no parquet under {path} — run cl_ine_ene_clean.py --clean"
        )

    schemas, rows = set(), 0
    for parquet in files:
        handle = pq.ParquetFile(parquet)
        schema = handle.schema_arrow
        schemas.add((tuple(schema.names), tuple(str(t) for t in schema.types)))
        rows += handle.metadata.num_rows
    if len(schemas) != 1:
        raise SystemExit(
            f"{len(schemas)} distinct schemas across partitions; expected 1"
        )

    names, types = next(iter(schemas))
    if set(types) != {"string"}:
        raise SystemExit(
            f"staging parquet must be all-STRING, found {sorted(set(types))}"
        )
    if "ano" in names or "mes" in names:
        raise SystemExit("ano/mes must be hive keys only, not stored columns")

    print(
        f"{len(files)} partitions, {rows:,} rows, {len(names)} all-STRING columns"
    )
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--check", action="store_true", help="verify locally, upload nothing"
    )
    parser.add_argument(
        "--dump-mode", default="overwrite", choices=["overwrite", "append"]
    )
    args = parser.parse_args()

    data_path = DATA / "output" / TABLE_ID
    verify_local(data_path)
    if args.check:
        return

    from pipelines.utils.tasks import _upload_to_gcs

    print(
        f"uploading -> gs://{BUCKET}/staging/{DATASET_ID}/{TABLE_ID}/ "
        f"(dump_mode={args.dump_mode})"
    )
    _upload_to_gcs(
        data_path=data_path,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        bucket_name=BUCKET,
        dump_mode=args.dump_mode,
        source_format="parquet",
    )
    print("done")


if __name__ == "__main__":
    main()
