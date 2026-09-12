"""Download, clean and write every by_year file for world_noaa_ghcn.

    python models/world_noaa_ghcn/code/backfill.py                # 1763-2026
    python models/world_noaa_ghcn/code/backfill.py --years 2020,2021
    python models/world_noaa_ghcn/code/backfill.py --from 1990 --to 2000

Disk discipline: years are processed in small batches — downloaded in parallel,
cleaned serially, and **each .csv.gz is deleted as soon as its parquet is
written**. The 14 GB of source archives is never on disk at once.

Resumable: a year whose parquet already exists with the expected row count is
skipped, so an interrupted run can simply be re-invoked.

Each year's output is checked against ``code/year_row_counts.csv``, which was
produced by independently streaming and counting all 264 source files. A
mismatch raises rather than warning — a short partition is exactly the kind of
silent loss that survives every downstream check.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pyarrow.parquet as pq
import requests

sys.path.insert(0, str(Path(__file__).parent))

import clean
import constants as c

DATA = Path(
    os.environ.get(
        "GHCN_DATA_DIR", os.path.expanduser("~/Downloads/world_noaa_ghcn_data")
    )
)
RAW = DATA / "input" / "by_year"
OUTPUT = DATA / "output"
EXPECTED_CSV = Path(__file__).parent / "year_row_counts.csv"
BATCH = 6


def expected_rows() -> dict[int, int]:
    """Expected row count per year, from the independent pre-build count.

    Returns:
        Mapping of year to the number of rows that year's source file holds.
    """
    with open(EXPECTED_CSV, encoding="utf-8") as fh:
        return {
            int(r["year"]): int(r["total_rows"]) for r in csv.DictReader(fh)
        }


def parquet_path(year: int) -> Path:
    """Output path for one year's partition.

    Args:
        year: Calendar year.

    Returns:
        Path to that year's ``data.parquet``.
    """
    return OUTPUT / "observation" / f"year={year}" / "data.parquet"


def already_done(year: int, expect: int) -> bool:
    """Whether a year's partition exists with the expected number of rows.

    Args:
        year: Calendar year.
        expect: Expected row count for that year.

    Returns:
        True when the partition can be skipped on a resume.
    """
    p = parquet_path(year)
    if not p.exists():
        return False
    try:
        return pq.ParquetFile(p).metadata.num_rows == expect
    except Exception:
        return False


def remote_size(url: str) -> int | None:
    """Content-Length of the source file, or None if the server withholds it."""
    r = requests.head(url, timeout=(30, 120), allow_redirects=True)
    r.raise_for_status()
    n = r.headers.get("Content-Length")
    return int(n) if n else None


def download(year: int, attempts: int = 4) -> Path:
    """Fetch one by_year archive, verifying it arrived whole.

    `requests.iter_content` ends silently when the connection drops mid-stream,
    so a partial body looks like a successful download. That is not theoretical:
    a first pass over this archive truncated 1991 to 99.7 MB of 152.7 MB, and
    three neighbouring years with it, purely from running six downloads at once.

    Two consequences, both handled here. The written file is compared against
    Content-Length and a short one is deleted and retried, so a truncation never
    becomes a short partition. And an *existing* file is re-checked against the
    same length rather than trusted, so a resume cannot silently reuse a
    truncated file from an earlier run.
    """
    RAW.mkdir(parents=True, exist_ok=True)
    dest = RAW / f"{year}.csv.gz"
    url = c.BY_YEAR_URL.format(year=year)
    want = remote_size(url)

    if dest.exists() and (want is None or dest.stat().st_size == want):
        return dest
    if dest.exists():
        print(
            f"  {year}: local {dest.stat().st_size:,} != remote {want:,}, refetching",
            flush=True,
        )
        dest.unlink()

    tmp = dest.with_suffix(".part")
    last = ""
    for attempt in range(1, attempts + 1):
        with requests.get(url, stream=True, timeout=(30, 1800)) as r:
            r.raise_for_status()
            # decode_content is left alone: the server sends the file already
            # gzipped and we want the gzip bytes on disk, not the decoded CSV.
            with open(tmp, "wb") as fh:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    fh.write(chunk)
        got = tmp.stat().st_size
        if want is None or got == want:
            tmp.rename(dest)
            return dest
        last = f"got {got:,} of {want:,} bytes"
        print(
            f"  {year}: truncated ({last}), attempt {attempt}/{attempts}",
            flush=True,
        )
        tmp.unlink()
        time.sleep(5 * attempt)
    raise OSError(
        f"{year}: download truncated after {attempts} attempts ({last})"
    )


def main() -> None:
    """Download, clean and write the requested years."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", help="comma-separated list")
    ap.add_argument("--from", dest="start", type=int, default=1763)
    ap.add_argument("--to", dest="end", type=int, default=2026)
    args = ap.parse_args()

    expect = expected_rows()
    if args.years:
        years = [int(y) for y in args.years.split(",")]
        # An unknown year would die later on `expect[y]`, and a duplicate would
        # start two downloads sharing one .part and one output path -- the
        # second racing the first year's archive deletion. Reject both up front.
        unknown = sorted(set(years) - expect.keys())
        if unknown:
            ap.error(
                f"no expected row count for {unknown}; not in year_row_counts.csv"
            )
        if len(years) != len(set(years)):
            ap.error("--years contains duplicates")
    else:
        years = [y for y in range(args.start, args.end + 1) if y in expect]

    t0 = time.time()
    written = skipped = 0
    total_rows = 0
    for i in range(0, len(years), BATCH):
        batch = years[i : i + BATCH]
        todo = [y for y in batch if not already_done(y, expect[y])]
        for y in batch:
            if y not in todo:
                skipped += 1
                total_rows += expect[y]
        if not todo:
            continue
        with ThreadPoolExecutor(max_workers=len(todo)) as ex:
            paths = dict(zip(todo, ex.map(download, todo), strict=True))
        for y in todo:
            n = clean.clean_year(y, paths[y], OUTPUT)
            if n != expect[y]:
                raise ValueError(
                    f"{y}: wrote {n:,} rows but year_row_counts.csv expects "
                    f"{expect[y]:,}. Refusing to continue with a short partition."
                )
            paths[y].unlink()
            written += 1
            total_rows += n
            done = written + skipped
            el = time.time() - t0
            print(
                f"[{done:>3}/{len(years)}] {y}  {n:>12,} rows  "
                f"total {total_rows / 1e9:>6.3f}bn  {el / 60:>6.1f} min",
                flush=True,
            )

    print(
        f"\ndone: {written} years written, {skipped} skipped, "
        f"{total_rows:,} rows in {(time.time() - t0) / 60:.1f} min"
    )


if __name__ == "__main__":
    main()
