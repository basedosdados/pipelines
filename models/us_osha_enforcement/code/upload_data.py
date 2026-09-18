#!/usr/bin/env python
"""Upload the cleaned Parquet to the ``basedosdados-dev`` staging dataset.

    GOOGLE_APPLICATION_CREDENTIALS=~/.basedosdados/credentials/staging.json \
    PYTHONPATH=$PWD python models/us_osha_enforcement/code/upload_data.py --all

Uses ``pipelines.utils.tasks._upload_to_gcs`` — the same helper the recurring
flow calls — rather than ``bd.Table.create`` on the data. Two reasons:

* ``bd.Table.create`` reads the whole Parquet into pandas and stringifies it,
  which on a 13M-row table balloons RAM into the tens of GB. The helper hands
  it a zero-row header instead and streams the data.
* A ``load_table_from_uri`` upload would leave staging as a NATIVE table. This
  dataset gets a recurring pipeline, and a pipeline only writes GCS objects — a
  native table would ignore them forever, and dbt would keep serving the
  bootstrap snapshot with no error, no warning and no failing test.

After each table the staging schema is checked: EXTERNAL, and every column
STRING. Both are silent failure modes otherwise.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from google.cloud import bigquery

from pipelines.datasets.us_osha_enforcement.constants import constants

log = logging.getLogger("us_osha_enforcement.upload")

PROJECT = "basedosdados-dev"
BUCKET = "basedosdados-dev"
STAGING = f"{constants.DATASET_ID.value}_staging"


def _assert_output_layout(table_dir: Path, partitioned: bool) -> None:
    """Fail before upload on a layout that would poison the staging schema.

    A zero-row first partition makes ``dump_header`` infer INTEGER columns and
    every real partition then fails to read, naming a column that looks
    arbitrary. A stray file at the prefix root of a hive-partitioned table
    carries no partition key and BigQuery refuses every read of the table.
    """
    if not table_dir.is_dir():
        raise FileNotFoundError(f"{table_dir} does not exist — clean it first")
    if not partitioned:
        return
    stray = [p.name for p in table_dir.iterdir() if p.is_file()]
    if stray:
        raise RuntimeError(
            f"{table_dir.name}: files at the partition root would break the "
            f"hive-partitioned staging table: {stray}"
        )
    parts = sorted(table_dir.glob("year=*/data.parquet"))
    if not parts:
        raise RuntimeError(f"{table_dir.name}: no partitions found")
    import pyarrow.parquet as pq

    first = pq.ParquetFile(parts[0])
    if first.metadata.num_rows == 0:
        raise RuntimeError(
            f"{table_dir.name}: {parts[0].parent.name} has zero rows; "
            "dump_header would infer INTEGER for every column"
        )


def _ensure_dataset(client: bigquery.Client) -> None:
    """Create ``<dataset>`` and ``<dataset>_staging`` if they do not exist.

    A brand-new staging dataset also needs the table-approve service account
    granted on it before a merge can materialise prod; that grant is a
    separate, manual step and this script cannot make it.
    """
    from google.api_core.exceptions import NotFound

    for name in (constants.DATASET_ID.value, STAGING):
        ref = f"{PROJECT}.{name}"
        try:
            client.get_dataset(ref)
        except NotFound:
            ds = bigquery.Dataset(ref)
            ds.location = "US"
            client.create_dataset(ds)
            log.warning(
                f"created BigQuery dataset {ref} — a NEW staging dataset needs "
                "the table-approve service account granted on it before the "
                "onboarding PR can materialise prod"
            )


def _assert_staging(client: bigquery.Client, table_slug: str) -> int:
    ref = f"{PROJECT}.{STAGING}.{table_slug}"
    tbl = client.get_table(ref)
    if tbl.table_type != "EXTERNAL":
        raise RuntimeError(
            f"{table_slug}: staging is {tbl.table_type}, not EXTERNAL — a "
            "recurring pipeline writes only GCS files and a native table "
            "would ignore them"
        )
    typed = [f.name for f in tbl.schema if f.field_type != "STRING"]
    if typed:
        raise RuntimeError(
            f"{table_slug}: staging columns are not STRING: {typed}"
        )
    rows = next(
        iter(client.query(f"select count(*) n from `{ref}`").result())
    ).n
    return int(rows)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--data-dir", default=constants.DEFAULT_DATA_DIR.value)
    p.add_argument("--all", action="store_true")
    p.add_argument("--tables", nargs="*", default=None)
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        p.error(
            "set GOOGLE_APPLICATION_CREDENTIALS to the staging service account"
        )

    from pipelines.utils.tasks import _upload_to_gcs

    output_dir = Path(args.data_dir).expanduser() / "output"
    tables = args.tables or (constants.TABLES.value if args.all else None)
    if not tables:
        p.error("pass --all or --tables")

    client = bigquery.Client(project=PROJECT)
    _ensure_dataset(client)
    counts: dict[str, int] = {}
    for slug in tables:
        table_dir = output_dir / slug
        partitioned = slug != "dicionario"
        _assert_output_layout(table_dir, partitioned)
        log.info(f"{slug}: uploading {table_dir}")
        # A pre-existing NATIVE table would keep the helper on its "already
        # exists" branch and never become external.
        client.query(
            f"drop table if exists `{PROJECT}.{STAGING}.{slug}`"
        ).result()
        _upload_to_gcs(
            data_path=str(table_dir),
            dataset_id=constants.DATASET_ID.value,
            table_id=slug,
            bucket_name=BUCKET,
            dump_mode="append",
            source_format="parquet",
        )
        counts[slug] = _assert_staging(client, slug)
        log.info(f"{slug}: {counts[slug]:,} rows in staging")

    log.info("=== staging row counts ===")
    for slug, rows in counts.items():
        log.info(f"  {slug:<22} {rows:>12,}")
    log.info(f"  {'TOTAL':<22} {sum(counts.values()):>12,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
