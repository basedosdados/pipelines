"""Download the Census Building Permits Survey ASCII files.

Writes to ``$BPS_DATA_DIR`` (default ``~/Downloads/us_census_bps_data/input``).

Two Census server behaviours make a naive download lose data silently:

* A file that does not exist is served as an HTML page. It does carry HTTP
  404, but the body must still be checked so an HTML page is never written to
  disk as if it were data.
* A small number of perfectly valid URLs are rejected by the site firewall,
  which answers **HTTP 200** with a "Request Rejected" HTML page. Treating
  that as a missing file drops whole months without any error — four place
  months and one metropolitan month were lost this way on the first run.
  The rejection is cached against the exact URL, so a cache-busting query
  string gets the real file.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.us_census_bps.constants import constants

BASE = constants.BASE_URL.value
HEADERS = constants.HEADERS.value
THIS_YEAR = time.gmtime().tm_year

DATA_DIR = Path(
    os.environ.get(
        "BPS_DATA_DIR", Path.home() / "Downloads/us_census_bps_data"
    )
)
INPUT_DIR = DATA_DIR / "input"


def targets() -> list[tuple[str, Path]]:
    """Build the (url, destination) list for every published BPS file."""
    out: list[tuple[str, Path]] = []
    for level, directory in constants.GEO_DIRS.value.items():
        prefix = constants.GEO_PREFIXES.value[level]
        for kind in ("monthly", "annual"):
            first = constants.FIRST_YEAR.value[(level, kind)]
            for year in range(first, THIS_YEAR + 1):
                names = (
                    [
                        f"{prefix}{year % 100:02d}{m:02d}c.txt"
                        for m in range(1, 13)
                    ]
                    if kind == "monthly"
                    else [f"{prefix}{year}a.txt"]
                )
                for name in names:
                    url = BASE + urllib.parse.quote(f"{directory}/{name}")
                    out.append((url, INPUT_DIR / level / name))
    for directory, prefix in constants.PLACE_REGIONS.value.items():
        for kind in ("monthly", "annual"):
            first = constants.FIRST_YEAR.value[("place", kind)]
            for year in range(first, THIS_YEAR + 1):
                names = (
                    [
                        f"{prefix}{year % 100:02d}{m:02d}c.txt"
                        for m in range(1, 13)
                    ]
                    if kind == "monthly"
                    else [f"{prefix}{year}a.txt"]
                )
                for name in names:
                    url = BASE + urllib.parse.quote(
                        f"Place/{directory}/{name}"
                    )
                    out.append((url, INPUT_DIR / "place" / name))
    return out


def fetch(url: str, dest: Path, tries: int = 5) -> tuple[str, str]:
    """Download one file.

    Args:
        url: Absolute URL of the Census file.
        dest: Destination path; parent directories are created.
        tries: Attempts before giving up.

    Returns:
        ``(status, detail)`` where status is "ok", "cached", "missing" or
        "failed". A "missing" result means the server answered HTTP 404.
    """
    if dest.exists() and dest.stat().st_size > 0:
        return "cached", ""
    detail = ""
    for attempt in range(tries):
        # The firewall caches its rejection against the exact URL, so every
        # retry after the first carries a different cache-busting parameter.
        target = url if attempt == 0 else f"{url}?attempt={attempt}"
        req = urllib.request.Request(target, headers=HEADERS)
        try:
            with urllib.request.urlopen(req, timeout=300) as resp:
                body = resp.read()
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return "missing", "HTTP 404"
            detail = f"HTTP {exc.code}"
            time.sleep(2 * (attempt + 1))
            continue
        except Exception as exc:
            detail = f"{type(exc).__name__}: {exc}"
            time.sleep(2 * (attempt + 1))
            continue
        head = body[:1000].lower()
        if b"<html" in head or b"<!doctype" in head:
            # HTTP 200 with an HTML body is the firewall, never a real file.
            detail = "firewall rejection (HTTP 200 with HTML body)"
            time.sleep(1 + attempt)
            continue
        if not body.strip():
            detail = "empty body"
            time.sleep(1 + attempt)
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(body)
        return "ok", ""
    return "failed", detail


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=6)
    args = parser.parse_args()

    jobs = targets()
    print(f"{len(jobs)} candidate files -> {INPUT_DIR}")
    counts = {"ok": 0, "cached": 0, "missing": 0, "failed": 0}
    failures: list[str] = []
    with cf.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(fetch, u, d): u for u, d in jobs}
        for i, fut in enumerate(cf.as_completed(futures), start=1):
            status, detail = fut.result()
            counts[status] += 1
            if status == "failed":
                failures.append(f"{futures[fut]} -- {detail}")
            if i % 250 == 0:
                print(f"  {i}/{len(jobs)} {counts}", flush=True)
    print(f"done: {counts}")
    if failures:
        print("FAILED (not a 404 -- do not treat as missing):")
        for line in failures[:30]:
            print("  ", line)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
