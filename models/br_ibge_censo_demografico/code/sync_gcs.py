"""Sync cleaned Censo 2022 extracts to the sandbox GCS bucket.

Usage:
    GOOGLE_APPLICATION_CREDENTIALS=~/.basedosdados/credentials/sandbox-507414.json \
      uv run python models/br_ibge_censo_demografico/code/sync_gcs.py [--delete-local]
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from google.cloud import storage
from google.cloud.exceptions import Conflict, NotFound

sys.path.insert(0, str(Path(__file__).parent))
import constants as c

os.environ.setdefault(
    "GOOGLE_APPLICATION_CREDENTIALS",
    str(Path.home() / ".basedosdados" / "credentials" / "sandbox-507414.json"),
)

# Local trees that are reproducible and should live in GCS, then be deleted.
SYNC_DIRS = ("output", "docs")
SYNC_FILES = ("row_counts.json",)


def ensure_bucket(client: storage.Client) -> storage.Bucket:
    try:
        bucket = client.get_bucket(c.GCS_BUCKET)
        print(f"using existing gs://{c.GCS_BUCKET}", flush=True)
        return bucket
    except NotFound:
        try:
            bucket = client.create_bucket(c.GCS_BUCKET, location="US")
            print(f"created gs://{c.GCS_BUCKET}", flush=True)
            return bucket
        except Conflict:
            bucket = client.bucket(c.GCS_BUCKET)
            print(f"using existing gs://{c.GCS_BUCKET}", flush=True)
            return bucket


def local_files() -> list[tuple[Path, str]]:
    pairs: list[tuple[Path, str]] = []
    for dirname in SYNC_DIRS:
        root = c.DATA_ROOT / dirname
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if path.is_file():
                rel = path.relative_to(c.DATA_ROOT).as_posix()
                pairs.append((path, f"{c.GCS_PREFIX}/{rel}"))
    for name in SYNC_FILES:
        path = c.DATA_ROOT / name
        if path.is_file():
            pairs.append((path, f"{c.GCS_PREFIX}/{name}"))
    return pairs


def _upload_one(bucket: storage.Bucket, path: Path, blob_name: str) -> str:
    blob = bucket.blob(blob_name)
    if blob.exists():
        blob.reload()
        if blob.size == path.stat().st_size:
            return f"skip {path.relative_to(c.DATA_ROOT)}"
    blob.upload_from_filename(str(path))
    return f"put  {path.relative_to(c.DATA_ROOT)}"


def upload_all(bucket: storage.Bucket, pairs: list[tuple[Path, str]]) -> None:
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [
            pool.submit(_upload_one, bucket, path, name)
            for path, name in pairs
        ]
        for fut in as_completed(futures):
            print(f"  {fut.result()}", flush=True)
    remote = {
        b.name: b.size for b in bucket.list_blobs(prefix=f"{c.GCS_PREFIX}/")
    }
    missing = []
    for path, name in pairs:
        if name not in remote:
            missing.append(name)
        elif remote[name] != path.stat().st_size:
            missing.append(
                f"{name} size {remote[name]} != {path.stat().st_size}"
            )
    if missing:
        raise RuntimeError(f"GCS mismatch {len(missing)}: {missing[:5]}")
    print(
        f"verified {len(pairs)} objects in gs://{c.GCS_BUCKET}/{c.GCS_PREFIX}/",
        flush=True,
    )


def delete_local() -> None:
    for dirname in SYNC_DIRS:
        path = c.DATA_ROOT / dirname
        if path.exists():
            shutil.rmtree(path)
            print(f"deleted {path}")
    for name in SYNC_FILES:
        path = c.DATA_ROOT / name
        path.unlink(missing_ok=True)
    leftover_input = c.INPUT_DIR
    if leftover_input.exists():
        shutil.rmtree(leftover_input)
        print(f"deleted {leftover_input}")
    dbt_dir = c.DATA_ROOT / "dbt"
    if dbt_dir.exists():
        shutil.rmtree(dbt_dir)
        print(f"deleted {dbt_dir}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--delete-local",
        action="store_true",
        help="Remove local scratch after a verified GCS upload",
    )
    args = parser.parse_args()
    client = storage.Client(project=c.GCP_PROJECT)
    bucket = ensure_bucket(client)
    pairs = local_files()
    if not pairs:
        print("nothing local to upload")
        return
    print(f"uploading {len(pairs)} files", flush=True)
    upload_all(bucket, pairs)
    if args.delete_local:
        delete_local()
        print("local extracts removed")


if __name__ == "__main__":
    main()
