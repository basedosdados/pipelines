"""Download every VEC blob needed for the state-election tables.

The manifest is built from the container listing (``inventory/blobs.json``), never
from scraping the website. Files land under ``input/<blob path>`` so the on-disk
tree mirrors the container exactly and the provenance of every parsed row is the
blob name.
"""

from __future__ import annotations

import concurrent.futures as cf
import hashlib
import json
import os
import sys
import time
import urllib.parse
import urllib.request

CONTAINER = "https://itsitecoreblobvecprd01.blob.core.windows.net/public-files"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/126 Safari/537.36"
SCRATCH = os.environ.get(
    "VEC_DATA_ROOT",
    os.path.expanduser("~/Downloads/au_vic_vec_elections_data"),
)
INPUT = os.path.join(SCRATCH, "input")

STATE_YEARS = (2006, 2010, 2014, 2018)


def wanted(names: list[str]) -> list[str]:
    """The state-election subset of the container."""
    keep: set[str] = set()
    for n in names:
        low = n.lower()
        # HTML result pages for 2006/2010/2014/2018
        if any(
            low.startswith(f"historical-results/state{y}/")
            for y in STATE_YEARS
        ):
            if low.endswith((".html", ".xls", ".xlsx")):
                keep.add(n)
        # Legislative Council workbooks + assorted state files
        elif low.startswith("historical-results/files/"):
            if low.endswith((".xls", ".xlsx")):
                keep.add(n)
        # 2022 and by-election reports
        elif (
            low.startswith("state/reports/")
            and low.endswith((".xls", ".xlsx"))
        ) or (
            low.startswith("historical-results/general/")
            and "province2002" in low
        ):
            keep.add(n)
    return sorted(keep)


def fetch(name: str) -> tuple[str, int, str]:
    dest = os.path.join(INPUT, name)
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    if os.path.exists(dest) and os.path.getsize(dest) > 0:
        with open(dest, "rb") as handle:
            blob = handle.read()
        return name, len(blob), hashlib.md5(blob).hexdigest()
    url = CONTAINER + "/" + urllib.parse.quote(name)
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    last: Exception | None = None
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                blob = r.read()
            with open(dest, "wb") as h:
                h.write(blob)
            return name, len(blob), hashlib.md5(blob).hexdigest()
        except Exception as exc:
            last = exc
            time.sleep(1.5 * (attempt + 1))
    return name, -1, f"ERROR {last}"


def main() -> None:
    with open(os.path.join(SCRATCH, "inventory", "blobs.json")) as handle:
        blobs = json.load(handle)
    names = wanted([b["name"] for b in blobs])
    print(f"manifest: {len(names)} blobs", flush=True)
    os.makedirs(INPUT, exist_ok=True)
    results: dict[str, dict] = {}
    done = 0
    with cf.ThreadPoolExecutor(max_workers=12) as pool:
        for name, size, md5 in pool.map(fetch, names):
            results[name] = {"size": size, "md5": md5}
            done += 1
            if done % 200 == 0:
                print(f"  {done}/{len(names)}", flush=True)
    bad = {k: v for k, v in results.items() if v["size"] < 0}
    # A blob that returns the container's XML error document is not a file.
    tiny = {k: v for k, v in results.items() if 0 <= v["size"] < 400}
    path = os.path.join(SCRATCH, "inventory", "downloaded.json")
    with open(path, "w", encoding="utf-8") as h:
        json.dump(results, h, indent=1)
    print(
        f"downloaded {len(results)}  errors={len(bad)}  suspiciously_small={len(tiny)}"
    )
    for k in list(bad)[:10]:
        print("  ERROR", k, results[k]["md5"])
    for k in list(tiny)[:10]:
        print("  TINY ", k, results[k]["size"])
    print("->", path)


if __name__ == "__main__":
    sys.exit(main())
