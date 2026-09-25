"""Upload the cleaned MG parquet mirrors to the dev staging bucket.

WHY A PLAIN GCS OVERLAY, AND NOT `bd.Table.upload`
--------------------------------------------------
The four staging tables already exist in `basedosdados-dev.world_wb_mides_staging`
as EXTERNAL tables over `gs://basedosdados-dev/staging/world_wb_mides/<mirror>/*`.
Nothing needs creating: an object dropped under that prefix is visible to the
next query. `bd.Table.create` would additionally read a sample parquet through
pandas to infer columns, which on these files is a large, pointless allocation.

THE OVERLAY IS LOAD-BEARING, NOT INCIDENTAL
-------------------------------------------
File names are `<phase>_<year>_<ibge7>.parquet`, identical to what already sits
in the bucket from the original ingest. So an upload:

  * REPLACES a municipality-exercise we re-harvested,
  * ADDS the exercises that did not exist before (2022-2026),
  * and LEAVES UNTOUCHED any municipality-exercise we do not have.

That last case is the whole reason this is a copy and not a sync. TCE-MG has
retroactively withdrawn municipalities from 2017 and 2018 -- the portal now
serves 711 of 853 for `empenho` 2017 and 696 for `despesa` 2017, 752 for 2018 --
so a mirror-and-delete would erase ~2.9M rows that MiDES already publishes.
Overlaying keeps the old vintage for exactly the municipalities that are missing
and takes the new vintage everywhere else, which is the per-municipality union
that was chosen deliberately. **Never add a delete/sync flag to this script**
without revisiting that decision.

Checked before writing anything: the external tables' schemas match these parquet
files column for column, in order, all STRING on both sides.

Usage:
    python upload_mg.py --dry-run
    python upload_mg.py --workers 16
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from google.cloud import storage
from google.oauth2 import service_account

sys.path.insert(0, str(Path(__file__).resolve().parent))

# Derived from the cleaner, never hand-listed: it grew from 4 mirrors to 49 when
# the full source was onboarded, and a hardcoded tuple would have silently
# uploaded a stale subset. A directory on disk that is NOT a current mirror (a
# renamed table, say) is therefore never picked up.
# pyrefly: ignore [missing-import]  # sibling module via sys.path
import clean_mg as _clean_mg

MIRRORS = tuple(sorted(_clean_mg.MIRROR.values()))
BUCKET = "basedosdados-dev"
PREFIX = "staging/world_wb_mides"
CREDENTIALS = Path.home() / ".basedosdados/credentials/staging.json"
OUTPUT = (
    Path(
        os.environ.get(
            "MIDES_DATA_DIR", Path.home() / "Downloads/world_wb_mides_data"
        )
    )
    / "output"
)


def client_and_bucket():
    info = json.loads(CREDENTIALS.read_text())
    creds = service_account.Credentials.from_service_account_file(
        str(CREDENTIALS)
    )
    client = storage.Client(credentials=creds, project=info["project_id"])
    # The staging bucket is requester-pays; without user_project every call is a
    # 400 that reads like a permissions problem.
    return client, client.bucket(BUCKET, user_project=info["project_id"])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--mirror", action="append", choices=MIRRORS)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--allow-incomplete",
        action="store_true",
        help="upload even if clean_mg.py has not written its completion sentinel",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help=(
            "Skip files already uploaded from THIS clean. A remote object counts "
            "as current only when its `updated` is at or after the local file's "
            "mtime, so a stale object from an earlier clean is still re-sent. "
            "This only ever skips work -- it never deletes, so the overlay "
            "semantics described above are unchanged."
        ),
    )
    args = parser.parse_args()

    # Refuse to upload a half-written output tree. `clean_mg.py` writes this
    # sentinel only after it exits successfully; a mirror count and a `du -sh`
    # look correct long before the clean is done, because every mirror directory
    # is created early and fills up over the following hour. An upload started
    # against a partial tree silently omits whatever the clean had not yet
    # written -- the last exercise, typically -- and nothing downstream says so:
    # the tables build, the tests pass, and the newest year is simply absent.
    sentinel = OUTPUT / ".clean_complete"
    if not sentinel.exists() and not args.allow_incomplete:
        raise SystemExit(
            f"{sentinel} is missing: clean_mg.py has not finished successfully.\n"
            "Wait for it, or pass --allow-incomplete if you really mean to upload "
            "a partial tree."
        )
    if sentinel.exists():
        newest = max(
            (p.stat().st_mtime for p in OUTPUT.glob("*/*.parquet")),
            default=0.0,
        )
        if newest > sentinel.stat().st_mtime:
            raise SystemExit(
                "parquet files are NEWER than the completion sentinel -- the tree "
                "changed after the clean finished. Re-run clean_mg.py."
            )
        recorded = json.loads(sentinel.read_text())
        if recorded.get("problems"):
            # Not fatal: the clean reached the end, it just flagged something.
            print(
                f"note: the clean reported {recorded['problems']} structural "
                "problem(s); see its log before trusting this upload"
            )

    client, bucket = client_and_bucket()
    mirrors = args.mirror or list(MIRRORS)

    plan: list[tuple[Path, str]] = []
    skipped = 0
    for mirror in mirrors:
        directory = OUTPUT / mirror
        files = sorted(directory.glob("*.parquet"))
        if not files:
            print(f"  {mirror}: nothing on disk, skipping")
            continue
        current: dict[str, float] = {}
        if args.resume:
            for blob in client.list_blobs(
                bucket, prefix=f"{PREFIX}/{mirror}/"
            ):
                current[blob.name.rsplit("/", 1)[-1]] = (
                    blob.updated.timestamp()
                )
        picked = 0
        size = 0
        for path in files:
            if (
                args.resume
                and current.get(path.name, 0.0) >= path.stat().st_mtime
            ):
                skipped += 1
                continue
            plan.append((path, f"{PREFIX}/{mirror}/{path.name}"))
            picked += 1
            size += path.stat().st_size
        print(f"  {mirror:<22} {picked:>6} files  {size / 1e9:>6.2f} GB")

    total_bytes = sum(p.stat().st_size for p, _ in plan)
    if skipped:
        print(f"\nresume: {skipped:,} objects already current, skipping")
    print(
        f"\n{len(plan):,} objects, {total_bytes / 1e9:.2f} GB -> gs://{BUCKET}/{PREFIX}/"
    )
    if not plan:
        print("nothing to upload")
        return
    if args.dry_run:
        print("dry run, nothing uploaded")
        return

    done = {"n": 0, "bytes": 0, "failed": 0}
    lock = threading.Lock()
    started = time.time()

    def send(item):
        path, key = item
        for attempt in range(4):
            try:
                bucket.blob(key).upload_from_filename(str(path))
                with lock:
                    done["n"] += 1
                    done["bytes"] += path.stat().st_size
                    if done["n"] % 500 == 0:
                        elapsed = max(time.time() - started, 1)
                        rate = done["bytes"] / elapsed
                        left = (len(plan) - done["n"]) / max(
                            done["n"] / elapsed, 1e-9
                        )
                        print(
                            f"  {done['n']:>6}/{len(plan)}  "
                            f"{done['bytes'] / 1e9:>6.2f} GB  "
                            f"{rate / 1e6:>5.1f} MB/s  eta {left / 60:>5.1f} min",
                            flush=True,
                        )
                return
            except Exception as exc:
                if attempt == 3:
                    with lock:
                        done["failed"] += 1
                    print(f"  FAILED {key}: {str(exc)[:120]}", flush=True)
                    return
                time.sleep(2**attempt)

    with ThreadPoolExecutor(args.workers) as pool:
        list(pool.map(send, plan))

    elapsed = time.time() - started
    print(
        f"\nuploaded {done['n']:,} objects, {done['bytes'] / 1e9:.2f} GB "
        f"in {elapsed / 60:.1f} min | failed {done['failed']}"
    )
    if done["failed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
