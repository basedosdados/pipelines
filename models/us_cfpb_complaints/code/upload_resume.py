"""Resume the staging upload of us_cfpb_complaints, blob by blob, with verification.

`bd.Storage.upload` sends every file in one pass and has no resume: a dropped
connection part-way (observed here as `BrokenPipeError` after 7 of 16 partitions)
leaves the GCS prefix half-populated, and re-running it re-sends everything.

This uploads each local parquet to its exact destination name, skips a blob that is
already present at the identical byte size, retries transient failures, and then
asserts that every expected blob exists at the right size. A partition that cannot
be uploaded raises — it is never skipped silently, because a missing blob under an
EXTERNAL table is invisible: the table simply returns fewer rows.

Run `upload.py` first so the EXTERNAL staging table exists; this only fills the
prefix it points at.
"""

import argparse
import os
import sys
import time
from pathlib import Path

import pyarrow.parquet as pq
from common import COMPLAINT, DATASET_ID, OUTPUT
from google.api_core import exceptions as gexc
from google.cloud import bigquery, storage

BILLING_PROJECT = "basedosdados-dev"
BUCKET = "basedosdados-dev"
CHUNK = 64 * 1024 * 1024
MAX_TRIES = 5


def destinations(table: str) -> list[tuple[Path, str]]:
    """Local parquet -> staging blob name, mirroring the hive layout exactly."""
    root = OUTPUT / table
    out = []
    for p in sorted(root.rglob("*.parquet")):
        rel = p.relative_to(root).as_posix()
        out.append((p, f"staging/{DATASET_ID}/{table}/{rel}"))
    return out


def upload_one(bucket, local: Path, name: str) -> str:
    blob = bucket.blob(name)
    blob.chunk_size = CHUNK
    size = local.stat().st_size
    existing = bucket.get_blob(name)
    if existing is not None and existing.size == size:
        return "skip"
    for attempt in range(1, MAX_TRIES + 1):
        try:
            blob.upload_from_filename(str(local))
            return "sent"
        except (gexc.GoogleAPICallError, ConnectionError, OSError) as e:
            if attempt == MAX_TRIES:
                raise
            wait = 5 * attempt
            print(
                f"    attempt {attempt} failed ({type(e).__name__}), retry in {wait}s"
            )
            time.sleep(wait)
    raise RuntimeError("unreachable")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--table", default=COMPLAINT)
    a = ap.parse_args()

    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        sys.exit("GOOGLE_APPLICATION_CREDENTIALS is not set")

    client = storage.Client(project=BILLING_PROJECT)
    bucket = client.bucket(BUCKET, user_project=BILLING_PROJECT)

    todo = destinations(a.table)
    print(f"{len(todo)} local parquet file(s) for {a.table}")
    for local, name in todo:
        t0 = time.time()
        what = upload_one(bucket, local, name)
        mb = local.stat().st_size / 1e6
        print(
            f"  {what:4s} {name}  {mb:7.1f} MB  ({time.time() - t0:.0f}s)",
            flush=True,
        )

    # Verify: every expected blob present at the exact local size, and nothing else
    # under the prefix.
    prefix = f"staging/{DATASET_ID}/{a.table}/"
    remote = {b.name: b.size for b in client.list_blobs(bucket, prefix=prefix)}
    problems = []
    expected_rows = 0
    for local, name in todo:
        want = local.stat().st_size
        expected_rows += pq.ParquetFile(local).metadata.num_rows
        if name not in remote:
            problems.append(f"MISSING {name}")
        elif remote[name] != want:
            problems.append(
                f"SIZE {name}: remote {remote[name]} != local {want}"
            )
    stray = sorted(set(remote) - {n for _, n in todo})
    for s in stray:
        problems.append(f"STRAY {s}")
    if problems:
        print("\nPROBLEMS:")
        for p in problems:
            print("  " + p)
        sys.exit(1)
    print(f"\nall {len(todo)} blobs verified under gs://{BUCKET}/{prefix}")

    bq = bigquery.Client(project=BILLING_PROJECT)
    ref = f"{BILLING_PROJECT}.{DATASET_ID}_staging.{a.table}"
    tbl = bq.get_table(ref)
    got = next(iter(bq.query(f"select count(*) n from `{ref}`").result())).n
    print(f"staging: {got:,} rows, table_type={tbl.table_type}")
    if tbl.table_type != "EXTERNAL":
        sys.exit(f"staging table is {tbl.table_type}, expected EXTERNAL")
    if got != expected_rows:
        sys.exit(f"ROW COUNT MISMATCH: {got:,} != {expected_rows:,}")
    print("OK")


if __name__ == "__main__":
    main()
