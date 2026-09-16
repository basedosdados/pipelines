"""Download the Espírito Santo source files from dados.es.gov.br.

Three packages, all plain per-year CSV: `despesas` (SIGEFES execution, 16.3 GB),
`compras` (SIGA procurement) and `contratos`. Resumable and year-scoped, the same
shape as download_mg.

The CKAN resource list needs more care here than MG's did, because ES publishes the
same logical file more than once and nothing in the API says which copy is current:

  * `Despesas-2013.csv` exists as TWO resources, 514.1 MB modified 2026-09-05 and
    504.1 MB modified 2026-05-22. Keyed by name, one silently wins.
  * 2024 appears three times: `Despesas-2024.csv` (1421 MB, current) plus
    `Despesas-2024_1Semestre.csv` and `_2Semestre.csv` (632 + 763 MB, May). A
    `Despesas-*` glob double-counts the exercise.

Both are resolved by keeping, per (stem, year), only the most recently modified
resource whose name has no suffix after the year.
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from constants import (
    BROWSER_UA,
    ES_ALWAYS_FETCH_STEMS,
    ES_CKAN,
    ES_DESPESA_FIRST_YEAR,
    ES_PACKAGES,
    ES_SKIP_STEMS,
    ES_TABLES,
    INPUT_DIR,
)

ES_INPUT = INPUT_DIR / "es"
CHUNK = 1 << 20

# `Despesas-2024.csv` and `Despesas-2024_1Semestre.csv`: stem, year, then anything
# else. The suffix group is what separates a current whole-year file from a stale
# half-year one.
RESOURCE_RE = re.compile(
    r"^(?P<stem>[A-Za-z]+)-(?P<year>\d{4})(?P<suffix>[^.]*)\.csv$",
    re.IGNORECASE,
)


def is_intact(path: Path, expected: int | None = None) -> bool:
    """True when the file is present and complete.

    These are plain CSV, so unlike MG's .csv.gz there is no CRC to lean on: a truncated
    transfer leaves a file that satisfies any "exists and is non-empty" check and only
    fails much later, mid-clean.

    The byte length against the source is both the cheapest and the strongest evidence
    available, so it is preferred. Parsing every row is the fallback for when the
    source declares no length -- correct, but it reads the whole 16 GB, which is far
    too expensive to spend on each resume check of an already-complete file.
    """
    if not path.exists() or path.stat().st_size == 0:
        return False
    if expected:
        return path.stat().st_size == expected
    try:
        with path.open(
            "r", encoding="utf-8-sig", errors="replace", newline=""
        ) as fh:
            # Read to the LAST row, not just past the header: a truncated file parses
            # perfectly for most of its length and only fails at the tail, so stopping
            # early would pass exactly the files this is meant to reject.
            rows = 0
            for _ in csv.reader(fh, delimiter=";"):
                rows += 1
        return rows > 1
    except (OSError, csv.Error):
        return False


def current_resources(
    session: requests.Session, package: str
) -> dict[str, tuple[str, int]]:
    """The resources to fetch from one package, de-duplicated.

    Keeps the newest resource per (stem, year) and drops any whose name carries a
    suffix after the year, which is how the stale semester copies of 2024 announce
    themselves.
    """
    r = session.get(ES_CKAN, params={"id": package}, timeout=180)
    r.raise_for_status()

    best: dict[tuple[str, int], tuple[str, str, str, int]] = {}
    for res in r.json()["result"]["resources"]:
        url = res.get("url") or ""
        name = url.rsplit("/", 1)[-1]
        m = RESOURCE_RE.match(res.get("name") or name)
        if not m:
            continue
        stem, year, suffix = m["stem"], int(m["year"]), m["suffix"]
        if stem in ES_SKIP_STEMS or stem not in ES_TABLES:
            continue
        if suffix:
            print(
                f"  [skip] {res.get('name')}: superseded by the whole-year file"
            )
            continue
        if not int(res.get("size") or 0):
            print(f"  [skip] {res.get('name')}: zero bytes at source")
            continue
        stamp = res.get("last_modified") or res.get("created") or ""
        key = (stem, year)
        if key in best and best[key][0] >= stamp:
            print(f"  [skip] {res.get('name')}: older duplicate ({stamp})")
            continue
        if key in best:
            print(
                f"  [dupe] {res.get('name')}: newer copy ({stamp}) supersedes "
                f"{best[key][0]}"
            )
        best[key] = (
            stamp,
            f"{stem}-{year}.csv",
            url,
            int(res.get("size") or 0),
        )

    return {fname: (url, size) for _, fname, url, size in best.values()}


def download(
    session: requests.Session,
    name: str,
    url: str,
    dest_dir: Path,
    expected: int = 0,
) -> bool:
    dest = dest_dir / name
    if is_intact(dest, expected):
        return True

    # Resume from a partial transfer rather than restarting it. These files run to
    # 1.4 GB and the connection drops often enough that a from-scratch retry can lose
    # hundreds of MB per attempt and never converge. CKAN redirects to S3
    # (one.s3.es.gov.br), which answers 206 with accept-ranges: bytes.
    tmp = dest.with_suffix(dest.suffix + ".part")
    have = tmp.stat().st_size if tmp.exists() else 0
    headers = {"Range": f"bytes={have}-"} if have else {}

    with session.get(url, stream=True, timeout=1800, headers=headers) as r:
        if r.status_code == 416:
            # Range past the end: the part is already the whole file.
            total = have
            mode, written = "ab", have
        elif have and r.status_code == 206:
            total = int(r.headers["Content-Range"].rsplit("/", 1)[-1])
            mode, written = "ab", have
            print(
                f"  resuming {name} at {have / 1e6:.0f} of {total / 1e6:.0f} MB"
            )
        else:
            # 200 means the server ignored the Range header, so anything already on
            # disk is not a prefix of this response and must be discarded.
            r.raise_for_status()
            total = int(r.headers.get("Content-Length") or 0) or expected
            mode, written = "wb", 0
        if r.status_code != 416:
            with open(tmp, mode) as fh:
                for chunk in r.iter_content(CHUNK):
                    fh.write(chunk)
                    written += len(chunk)

    if total and written != total:
        # Keep the .part: the next attempt resumes from exactly here.
        print(
            f"  {name}: short transfer, {written / 1e6:.0f} of {total / 1e6:.0f} MB"
        )
        return False
    tmp.replace(dest)
    if not is_intact(dest, total):
        dest.unlink(missing_ok=True)
        return False
    return True


def main(retries: int = 3, years: set[int] | None = None) -> None:
    ES_INPUT.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    session.headers.update({"User-Agent": BROWSER_UA})

    wanted: dict[str, tuple[str, int]] = {}
    for package in ES_PACKAGES.values():
        for name, meta in current_resources(session, package).items():
            if name in wanted:
                print(
                    f"  [skip] {name}: already taken from an earlier package"
                )
                continue
            wanted[name] = meta

    # Drop the 2004-2008 aggregate despesa files unconditionally: they are not
    # transaction grain and no downstream model reads them. See ES_DESPESA_FIRST_YEAR.
    pre = {
        n
        for n in wanted
        if n.startswith("Despesas-") and int(n[9:13]) < ES_DESPESA_FIRST_YEAR
    }
    for n in sorted(pre):
        print(f"  [skip] {n}: annual aggregate, not transaction grain")
        wanted.pop(n)

    print(f"{len(wanted)} resources across {len(ES_PACKAGES)} packages")

    # Year-scoped refresh, but only over the families whose file-name year really is
    # the exercise. The contratos family is bucketed by a date that may be missing --
    # `Contratos-1753.csv` is SQL Server's datetime floor standing in for "never
    # recorded", and holds 180 real contracts -- so scoping it by name would delete
    # rows rather than skip work. It is ~45 MB in total; always take it whole.
    if years is not None:
        scoped = {}
        for n, meta in wanted.items():
            m = RESOURCE_RE.match(n)
            stem = m["stem"] if m else ""
            if stem in ES_ALWAYS_FETCH_STEMS or (
                m and int(m["year"]) in years
            ):
                scoped[n] = meta
        print(
            f"  year filter {sorted(years)}: {len(scoped)} of {len(wanted)} "
            f"(contratos family always fetched whole)"
        )
        wanted = scoped

    pending = dict(wanted)
    for attempt in range(1, retries + 1):
        failed = {}
        for name, (url, size) in sorted(pending.items()):
            ok = False
            try:
                ok = download(session, name, url, ES_INPUT, size)
            except requests.RequestException as exc:
                print(f"  {name}: {type(exc).__name__}")
            if ok:
                print(f"  ok {name}")
                sys.stdout.flush()
                continue
            failed[name] = (url, size)
            print(f"  {name}: incomplete, will retry")
            sys.stdout.flush()
        if not failed:
            break
        pending = failed
        print(f"attempt {attempt}: {len(failed)} to retry")
    else:
        print(f"STILL FAILING after {retries} attempts: {sorted(pending)}")
        raise SystemExit(1)

    total = sum(p.stat().st_size for p in ES_INPUT.glob("*.csv"))
    print(f"OK: {len(wanted)} files, {total / 1e9:.2f} GB")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--retries", type=int, default=3)
    ap.add_argument(
        "--year",
        type=int,
        action="append",
        help="restrict to this exercise; repeatable. Every ES file is per-year, so "
        "unlike MG there are no whole-table dimension exports to always fetch.",
    )
    args = ap.parse_args()
    main(retries=args.retries, years=set(args.year) if args.year else None)
