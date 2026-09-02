"""Upload the cleaned us_stanford_dime Parquet to BigQuery staging.

The contribution table is ~861M rows and its Parquet runs to roughly 82 GB,
which does not fit on the machine doing the conversion. So the contribution
path never holds a whole cycle on disk: DuckDB's ``COPY`` writes size-capped
part files as it goes, and a watcher ships each completed part to GCS and
deletes it while the conversion is still running. Peak local usage stays at the
source file plus a couple of parts.

Two deliberate departures from ``bd.Table.create``:

* ``bd.Table.create`` reads the whole Parquet into pandas before staging it,
  which balloons RAM into the tens of GB on files this size. Streaming the blob
  to GCS and issuing a server-side ``load_table_from_uri`` keeps RAM flat.
* A load job refuses to overwrite an EXTERNAL table, so any staging table left
  behind by an earlier ``bd.Table.create`` is dropped first. dbt reads either
  kind, and table-approve rebuilds prod staging as EXTERNAL itself.

A 0-row ``00_header.parquet`` is written into every staging prefix. The
table-approve action reads the *first* blob in the prefix with
``pd.read_parquet`` purely to learn the column names; pointing it at an empty
file is what stops it OOM-killing the CI runner on a large first part.

Requires GOOGLE_APPLICATION_CREDENTIALS pointing at a Data Basis dev service
account. The GCS bucket is requester-pays, so the client is pinned to a billing
project.
"""

from __future__ import annotations

import argparse
import contextlib
import os
import subprocess
import sys
import time
from pathlib import Path

import google.cloud.storage as gcs
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery

sys.path.insert(0, str(Path(__file__).resolve().parent))
import architecture as arch
import clean

BILLING_PROJECT = "basedosdados-dev"
BUCKET = "basedosdados-dev"
DATASET_ID = "us_stanford_dime"
STAGING_DATASET = f"{DATASET_ID}_staging"
CHUNK_SIZE = 256 * 1024 * 1024

_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket


def _storage() -> gcs.Client:
    return gcs.Client(project=BILLING_PROJECT)


def staging_prefix(table: str) -> str:
    return f"staging/{DATASET_ID}/{table}"


def upload_blob(
    local: Path, blob_name: str, client: gcs.Client | None = None
) -> None:
    """Stream one local file to GCS without loading it into memory."""
    client = client or _storage()
    blob = client.bucket(BUCKET).blob(blob_name)
    blob.chunk_size = CHUNK_SIZE
    blob.upload_from_filename(str(local))


def write_header_blob(table: str, client: gcs.Client | None = None) -> None:
    """Write a 0-row Parquet carrying the table's exact all-STRING schema.

    Sorts first in the prefix, so table-approve reads an empty file instead of a
    multi-GB one when it goes looking for column names.
    """
    schema = pa.schema(
        [(name, pa.string()) for name in arch.column_names(table)]
    )
    local = clean.SCRATCH / f"00_header_{table}.parquet"
    pq.write_table(
        pa.table(
            {n: pa.array([], type=pa.string()) for n in schema.names},
            schema=schema,
        ),
        local,
        compression="snappy",
    )
    upload_blob(local, f"{staging_prefix(table)}/00_header.parquet", client)
    local.unlink(missing_ok=True)
    print(f"  header blob written for {table} ({len(schema.names)} columns)")


def clear_prefix(table: str, client: gcs.Client | None = None) -> int:
    """Delete every blob under a table's staging prefix.

    Stale parts from an interrupted run would otherwise be picked up by the
    wildcard load and silently inflate the row count.
    """
    client = client or _storage()
    bucket = client.bucket(BUCKET)
    blobs = list(client.list_blobs(bucket, prefix=staging_prefix(table) + "/"))
    for b in blobs:
        b.delete()
    return len(blobs)


def load_staging_table(table: str) -> int:
    """Load the staging prefix into BigQuery and return the row count."""
    bq = bigquery.Client(project=BILLING_PROJECT)
    ds = bigquery.Dataset(f"{BILLING_PROJECT}.{STAGING_DATASET}")
    ds.location = "US"
    bq.create_dataset(ds, exists_ok=True)
    target = f"{BILLING_PROJECT}.{STAGING_DATASET}.{table}"
    bq.query(f"drop table if exists `{target}`").result()
    job = bq.load_table_from_uri(
        f"gs://{BUCKET}/{staging_prefix(table)}/*.parquet",
        target,
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition="WRITE_TRUNCATE",
        ),
    )
    job.result()
    return bq.get_table(target).num_rows


# --------------------------------------------------------------------------
# streaming conversion + upload for the contribution table
# --------------------------------------------------------------------------


class ConversionError(RuntimeError):
    """The DuckDB conversion subprocess exited non-zero."""

    def __init__(self, cycle: int, code: int) -> None:
        super().__init__(f"conversion of cycle {cycle} failed (exit {code})")
        self.cycle = cycle
        self.code = code


def stream_cycle(cycle: int, keep_input: bool = False) -> tuple[int, int]:
    """Convert one contribution cycle, uploading parts as they are written.

    Returns ``(rows, parts)``.

    The conversion runs as a **separate process**, not a thread. DuckDB's COPY
    and this uploader would otherwise share one interpreter, and the uploader's
    polling holds the GIL often enough to stall the conversion outright — an
    earlier threaded version left a part file at zero bytes for six minutes
    while the poller burned 93% of a core.

    A part is only shipped once it is complete. DuckDB appends to the
    highest-numbered part, so every lower-numbered one is finished; the last is
    shipped after the process exits. A part that is still zero-length, or whose
    footer will not parse yet, is left for the next pass.
    """
    src = clean.INPUT / f"contribDB_{cycle}.csv.gz"
    if not src.exists():
        raise FileNotFoundError(f"missing source file {src}")
    out_dir = clean.OUTPUT / "contribution" / str(cycle)
    out_dir.mkdir(parents=True, exist_ok=True)
    for stale in out_dir.glob("*.parquet"):
        stale.unlink()

    client = _storage()
    prefix = staging_prefix("contribution")
    proc = subprocess.Popen(
        [
            sys.executable,
            str(Path(__file__).resolve().parent / "clean.py"),
            "contribution",
            "--cycle",
            str(cycle),
            "--copy-only",
            str(out_dir),
        ],
    )

    def part_index(p: Path) -> int:
        return int(p.stem.rsplit("_", 1)[-1])

    rows = 0
    parts = 0
    shipped: set[str] = set()
    while True:
        finished = proc.poll() is not None
        present = sorted(
            (
                p
                for p in out_dir.glob("data_*.parquet")
                if p.name not in shipped
            ),
            key=part_index,
        )
        # Until the process exits, the highest-numbered part is still growing.
        ready = present if finished else present[:-1]
        progressed = False
        for f in ready:
            if f.stat().st_size == 0:
                continue
            try:
                n = pq.ParquetFile(f).metadata.num_rows
            except Exception:
                continue  # footer not written yet
            upload_blob(f, f"{prefix}/contribution_{cycle}_{f.name}", client)
            f.unlink()
            shipped.add(f.name)
            rows += n
            parts += 1
            progressed = True
        # Fail immediately rather than waiting on parts a dead process will
        # never finish writing.
        if finished and proc.returncode != 0:
            for leftover in out_dir.glob("*.parquet"):
                leftover.unlink()
            raise ConversionError(cycle, proc.returncode)
        if finished and not [
            p for p in out_dir.glob("data_*.parquet") if p.name not in shipped
        ]:
            break
        if not progressed:
            time.sleep(3)
    if not keep_input:
        src.unlink(missing_ok=True)
    with contextlib.suppress(OSError):
        out_dir.rmdir()
    return rows, parts


def upload_simple(table: str) -> tuple[int, int]:
    """Upload an already-cleaned single-file table's parts."""
    out_dir = clean.OUTPUT / table
    files = sorted(out_dir.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"no parquet under {out_dir}")
    client = _storage()
    prefix = staging_prefix(table)
    rows = 0
    for p in files:
        rows += pq.ParquetFile(p).metadata.num_rows
        upload_blob(p, f"{prefix}/{p.name}", client)
    return rows, len(files)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("table")
    p.add_argument(
        "--cycle", type=int, action="append", help="contribution cycles"
    )
    p.add_argument("--keep-input", action="store_true")
    p.add_argument(
        "--no-clear", action="store_true", help="append to the staging prefix"
    )
    p.add_argument(
        "--skip-load",
        action="store_true",
        help="upload only, no BigQuery load",
    )
    args = p.parse_args()

    client = _storage()
    if not args.no_clear:
        n = clear_prefix(args.table, client)
        print(
            f"cleared {n} existing blob(s) under {staging_prefix(args.table)}"
        )
    write_header_blob(args.table, client)

    total = 0
    if args.table == "contribution":
        cycles = args.cycle or clean.CYCLES
        for cycle in cycles:
            t0 = time.time()
            rows, parts = stream_cycle(cycle, keep_input=args.keep_input)
            total += rows
            free = os.statvfs(clean.SCRATCH)
            gb = free.f_bavail * free.f_frsize / 1e9
            print(
                f"cycle {cycle}: {rows:,} rows in {parts} part(s), "
                f"{time.time() - t0:.0f}s, {gb:.1f} GB free"
            )
    else:
        total, parts = upload_simple(args.table)
        print(f"{args.table}: {total:,} rows in {parts} part(s)")

    print(f"local parquet rows uploaded: {total:,}")
    if args.skip_load:
        return
    loaded = load_staging_table(args.table)
    print(f"BigQuery {STAGING_DATASET}.{args.table}: {loaded:,} rows")
    if loaded != total:
        raise SystemExit(
            f"ROW COUNT MISMATCH: uploaded {total:,} but BigQuery has {loaded:,}"
        )
    print("row counts match")


if __name__ == "__main__":
    main()
