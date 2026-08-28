"""Upload the br_bd_execucao_estadual staging parquet to BigQuery.

Usage:
    uv run python models/br_bd_execucao_estadual/code/upload.py [--env dev] [table ...]

Onboarding only ever uploads to **dev**. The prod tables are materialised by the
table-approve action when the PR merges -- never populate `basedosdados` by hand.

What lands here is the *staging mirror* of each source table (mg_ft_despesa,
mg_dm_empenho, ...), all columns STRING. The harmonization onto the canonical `despesa`
schema happens in the dbt models, not here, so the mapping stays reviewable in SQL rather
than buried in Python.

**Why this does not use `bd.Table.create`.** That helper reads the entire parquet into
pandas and `astype(str)`-stringifies it before writing to GCS. On `mg_ft_despesa`
(80.3M rows, ~1.9 GB) that balloons to tens of GB of RAM and can wedge the machine. Here
the files are streamed to GCS in 256 MB chunks and loaded server-side with
`load_table_from_uri`, which holds flat RAM regardless of table size. The parquet is
already all-STRING (duckdb `all_varchar`), so the stringify pass it skips was redundant
anyway and the resulting staging schema is identical.

**The 0-row header file.** The table-approve action rebuilds prod staging and calls
`pd.read_parquet` on the *first* blob in each prefix, in GCS lexicographic order, purely
to read column names -- loading the whole file to do it. A large first file OOM-kills the
CI runner, dbt never runs, and no prod table is built. Seeding `00_header.parquet`
(0 rows, identical schema, sorts first) makes that read free. It contributes no rows, so
counts are unaffected.
"""

from __future__ import annotations

import os
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import pyarrow.parquet as pq  # noqa: E402
from google.cloud import bigquery, storage  # noqa: E402

_argv = sys.argv[1:]
if "--env" in _argv:
    _i = _argv.index("--env")
    ENV = _argv[_i + 1]
    _argv = _argv[:_i] + _argv[_i + 2 :]
else:
    ENV = "dev"

BILLING_PROJECT = "basedosdados" if ENV == "prod" else "basedosdados-dev"
BUCKET = BILLING_PROJECT
DATASET_ID = "br_bd_execucao_estadual"
HEADER_NAME = "00_header.parquet"
CHUNK = 256 * 1024 * 1024

OUTPUT_ROOT = (
    Path(
        os.environ.get(
            "EXEC_ESTADUAL_DATA_DIR",
            Path.home() / "Downloads" / "br_state_budget_data",
        )
    )
    / "output"
)


def _bucket(client: storage.Client):
    # The bucket is requester-pays, so every handle needs an explicit billing project.
    return client.bucket(BUCKET, user_project=BILLING_PROJECT)


def _write_header(files: list[Path], dest: Path) -> Path:
    """A 0-row parquet carrying the table's exact schema."""
    schema = pq.read_schema(files[0])
    header = dest / HEADER_NAME
    pq.write_table(schema.empty_table(), header, compression="snappy")
    return header


def upload_table(slug: str, gcs: storage.Client, bq: bigquery.Client) -> int:
    src = OUTPUT_ROOT / slug
    files = sorted(p for p in src.glob("*.parquet") if p.name != HEADER_NAME)
    if not files:
        raise FileNotFoundError(f"no parquet under {src}")

    prefix = f"staging/{DATASET_ID}/{slug}"
    bucket = _bucket(gcs)

    # Clear the prefix first: a leftover file from an earlier run under a different layout
    # would be picked up by the load wildcard and silently double-count rows.
    stale = list(bucket.list_blobs(prefix=prefix + "/"))
    for blob in stale:
        blob.delete()
    if stale:
        print(f"  cleared {len(stale)} stale blobs")

    for path in [_write_header(files, src), *files]:
        blob = bucket.blob(f"{prefix}/{path.name}")
        blob.chunk_size = CHUNK
        blob.upload_from_filename(str(path))
    print(f"  uploaded {len(files) + 1} files", flush=True)

    # A load job refuses to write over a table of type EXTERNAL ("is not allowed for this
    # operation because it currently has type EXTERNAL"), which is what bd.Table.create
    # leaves behind. Drop first so the layout is whatever this script produces, not a
    # mixture of the two. Prod staging is rebuilt as EXTERNAL by table-approve regardless;
    # dbt reads either kind, and the 0-row header above is what keeps that rebuild alive.
    bq.query(
        f"drop table if exists `{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}`"
    ).result()

    job = bq.load_table_from_uri(
        f"gs://{BUCKET}/{prefix}/*.parquet",
        f"{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}",
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition="WRITE_TRUNCATE",
        ),
    )
    job.result()

    n = next(
        iter(
            bq.query(
                f"select count(*) as n from "
                f"`{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}`"
            ).result()
        )
    ).n
    print(f"  {slug}: {n:,} rows", flush=True)
    if n == 0:
        raise ValueError(f"{slug}: loaded 0 rows")
    return n


def discover() -> list[tuple[str, int]]:
    """Every built staging table, smallest first, with its on-disk byte size."""
    out = []
    for d in sorted(OUTPUT_ROOT.iterdir()):
        if not d.is_dir():
            continue
        size = sum(p.stat().st_size for p in d.glob("*.parquet"))
        if size:
            out.append((d.name, size))
    return sorted(out, key=lambda t: t[1])


def main() -> None:
    only = set(_argv)
    tables = [(s, b) for s, b in discover() if not only or s in only]
    if not tables:
        print("nothing to upload; run clean_mg.py first")
        sys.exit(1)

    gcs = storage.Client(project=BILLING_PROJECT)
    bq = bigquery.Client(project=BILLING_PROJECT)
    print(
        f"=== uploading {len(tables)} tables to {BILLING_PROJECT} (env={ENV}) ==="
    )
    for slug, size in tables:
        print(f"=== {slug} ({size / 1e6:.1f} MB) ===", flush=True)
        try:
            upload_table(slug, gcs, bq)
        except Exception as e:
            print(f"  FAILED: {type(e).__name__}: {e}")
            sys.exit(1)
    print("ALL TABLES UPLOADED")


if __name__ == "__main__":
    main()
