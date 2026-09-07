"""One-shot harvest of Australian Parliament Hansard XML (1901-present).

Two sources, because ParlInfo no longer serves its own back catalogue:

* **1901-2005** - the GLAM Workbench mirror (``wragge/hansard-xml``), harvested
  from ParlInfo in 2016 and re-run in 2024. Fetched with a shallow git clone.
  ParlInfo itself now answers most pre-1998 requests with a "Missing File" page
  and HTTP 200: measured 2026-09-02, it returns a real transcript for under 20%
  of pre-1998 sitting days, and 0% for the 1920s. The mirror is complete.
* **2006-present** - ParlInfo directly, probed one calendar day per chamber.
  Coverage here is 100%.

Resumable: existing files are left alone, so the script can be re-run.

Usage:
    python models/au_aph_hansard/code/harvest.py [--end 2026] [--skip-mirror]
"""

from __future__ import annotations

import argparse
import csv
import os
import shutil
import subprocess
import sys
import threading
import time
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.au_aph_hansard.constants import constants
from pipelines.datasets.au_aph_hansard.utils import (
    build_download_url,
    daterange,
    find_sitting_day_xml,
    http_get,
    is_hansard_xml,
)

DATA_ROOT = Path(
    os.environ.get(
        "AU_APH_HANSARD_DATA",
        Path.home() / "Downloads" / "au_aph_hansard_data",
    )
)
INPUT_DIR = DATA_ROOT / "input"
MIRROR_DIR = DATA_ROOT / "mirror"
MANIFEST = DATA_ROOT / "manifest.csv"

MIRROR_REPO = "https://github.com/wragge/hansard-xml.git"

_lock = threading.Lock()
_counter = {"ok": 0, "skip": 0, "missing": 0, "fail": 0, "nositting": 0}


def _bump(key: str) -> None:
    with _lock:
        _counter[key] += 1
        total = sum(_counter.values())
        if total % 500 == 0:
            print(f"  ... {total:>6} processed  {_counter}", flush=True)


def clone_mirror() -> Path:
    """Shallow-clone the 1901-2005 mirror (~4.6 GB) unless already present."""
    if (MIRROR_DIR / ".git").exists():
        print(f"Mirror already cloned at {MIRROR_DIR}", flush=True)
        return MIRROR_DIR
    MIRROR_DIR.parent.mkdir(parents=True, exist_ok=True)
    print(
        f"Cloning mirror into {MIRROR_DIR} (~4.6 GB, shallow) ...", flush=True
    )
    subprocess.run(
        [
            "git",
            "clone",
            "--depth",
            "1",
            "--single-branch",
            MIRROR_REPO,
            str(MIRROR_DIR),
        ],
        check=True,
    )
    return MIRROR_DIR


def collect_mirror(end_year: int) -> list[dict]:
    """Validate and stage every transcript the mirror holds."""
    records: list[dict] = []
    for house in ("hofreps", "senate"):
        for path in sorted((MIRROR_DIR / house).rglob("*.xml")):
            year = path.parent.name
            if not year.isdigit() or int(year) > end_year:
                continue
            payload = path.read_bytes()
            record = {
                "house": house,
                "year": year,
                "date": "",
                "file": path.name,
                "source": "mirror",
                "path": "",
                "status": "",
                "bytes": len(payload),
            }
            if not is_hansard_xml(payload):
                record["status"] = "missing"
                _bump("missing")
                records.append(record)
                continue
            dest = INPUT_DIR / house / year / path.name
            record["path"] = str(dest)
            if dest.exists() and dest.stat().st_size == len(payload):
                record["status"] = "skip"
                _bump("skip")
            else:
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(path, dest)
                record["status"] = "ok"
                _bump("ok")
            records.append(record)
    return records


def fetch_parlinfo(house: str, day: date) -> dict | None:
    """Probe ParlInfo for one chamber-day and download the transcript."""
    relative = find_sitting_day_xml(house, day)
    if relative is None:
        _bump("nositting")
        return None
    filename = relative.rstrip("/").split("/")[-1].split(";")[0]
    from urllib.parse import unquote

    filename = unquote(filename)
    dest = INPUT_DIR / house / str(day.year) / filename
    record = {
        "house": house,
        "year": str(day.year),
        "date": day.isoformat(),
        "file": filename,
        "source": "parlinfo",
        "path": str(dest),
        "status": "",
        "bytes": 0,
    }
    if dest.exists() and dest.stat().st_size > 0:
        record["status"] = "skip"
        record["bytes"] = dest.stat().st_size
        _bump("skip")
        return record
    try:
        payload = http_get(build_download_url(relative))
    except urllib.error.HTTPError as exc:
        record["status"] = "missing" if exc.code == 404 else f"http_{exc.code}"
        _bump("missing" if exc.code == 404 else "fail")
        return record
    except Exception as exc:
        record["status"] = f"error_{type(exc).__name__}"
        _bump("fail")
        return record
    if not is_hansard_xml(payload):
        record["status"] = "missing"
        _bump("missing")
        return record
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(payload)
    record["status"] = "ok"
    record["bytes"] = len(payload)
    _bump("ok")
    return record


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--end", type=int, default=date.today().year)
    parser.add_argument(
        "--probe-start", type=int, default=constants.INDEX_LAST_YEAR.value + 1
    )
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--skip-mirror", action="store_true")
    args = parser.parse_args()

    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    began = time.time()
    records: list[dict] = []

    if not args.skip_mirror:
        clone_mirror()
        print(
            "Phase A: staging mirror transcripts (1901-2005) ...", flush=True
        )
        records += collect_mirror(args.end)
        print(f"  mirror staged. {_counter}", flush=True)

    if args.probe_start <= args.end:
        first = date(args.probe_start, 1, 1)
        last = min(date(args.end, 12, 31), date.today())
        days = list(daterange(first, last))
        print(
            f"Phase B: probing ParlInfo, {len(days):,} days x 2 chambers "
            f"({args.probe_start}-{args.end})",
            flush=True,
        )
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = [
                pool.submit(fetch_parlinfo, house, day)
                for house in constants.CHAMBERS.value
                for day in days
            ]
            for future in as_completed(futures):
                result = future.result()
                if result:
                    records.append(result)

    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    with MANIFEST.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "house",
                "year",
                "date",
                "file",
                "source",
                "path",
                "status",
                "bytes",
            ],
        )
        writer.writeheader()
        writer.writerows(
            sorted(records, key=lambda r: (r["house"], r["year"], r["file"]))
        )

    kept = [r for r in records if r["status"] in {"ok", "skip"}]
    print(
        f"\nDone in {(time.time() - began) / 60:.1f} min. {_counter}\n"
        f"{len(kept):,} transcripts staged, {sum(r['bytes'] for r in kept) / 1e9:.2f} GB.\n"
        f"Manifest: {MANIFEST}",
        flush=True,
    )


if __name__ == "__main__":
    main()
