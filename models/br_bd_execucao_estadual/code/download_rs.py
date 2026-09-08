"""Download the Rio Grande do Sul source files from dados.rs.gov.br.

CAGE publishes one flat table as twelve monthly ZIPs per exercise, 2012-2026. Small to
fetch (~1.6 GB total, 20x compression) and slow to process (~36 GB expanded).

Two things need care here that the other states did not need:

1. **Reachability is path-dependent, not country-dependent.** `dados.rs.gov.br`
   resolves everywhere but refused a residential Australian ISP outright while
   answering fine from a university range. A timeout means "try another egress", not
   "the source is down", so failures are collected and reported rather than swallowed.

2. **The package slug convention changes four times** across the series
   (`despesas-do-estado-em-2012` ... `despesas-do-estado-2022` ... `despesas-2023` ...
   `2024-despesas-do-estado`). Packages are therefore discovered from the catalogue and
   matched on shape; a single f-string template silently misses whole exercises.

Resources are enumerated POSITIONALLY, never by name: the 2020 package lists "Agosto"
twice and has no "Setembro", so a name-keyed dict loses a month without a word.
"""

from __future__ import annotations

import argparse
import re
import sys
import zipfile
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from constants import BROWSER_UA, INPUT_DIR, RS_CKAN, RS_PACKAGE_LIST

RS_INPUT = INPUT_DIR / "rs"
CHUNK = 1 << 20
YEAR = re.compile(r"(?<!\d)(20\d{2})(?!\d)")


def is_intact(path: Path) -> bool:
    """A ZIP is intact only when every member's CRC checks out.

    `testzip()` decompresses each member, so a truncated download fails here rather
    than several steps later inside the cleaner. RS drops connections often enough on
    a long transfer that a size-only check is not enough.
    """
    if not path.exists() or path.stat().st_size == 0:
        return False
    try:
        with zipfile.ZipFile(path) as z:
            return z.testzip() is None
    except (zipfile.BadZipFile, OSError):
        return False


def despesa_packages(session: requests.Session) -> list[str]:
    r = session.get(RS_PACKAGE_LIST, timeout=120)
    r.raise_for_status()
    return sorted(
        n
        for n in r.json()["result"]
        if YEAR.search(n) and "despesa" in n.lower()
    )


def planned(
    session: requests.Session, years: set[int] | None
) -> list[tuple[str, str]]:
    """(destination file name, url) for every monthly archive to fetch."""
    out: list[tuple[str, str]] = []
    for package in despesa_packages(session):
        m = YEAR.search(package)
        year = int(m.group(1)) if m else 0
        if years is not None and year not in years:
            continue
        r = session.get(RS_CKAN, params={"id": package}, timeout=120)
        r.raise_for_status()
        for i, res in enumerate(r.json()["result"].get("resources", [])):
            url = res.get("url") or ""
            if not url.lower().endswith(".zip"):
                continue
            stem = Path(url).name or f"{year}_{i:02d}.zip"
            # The positional index is part of the name so two resources sharing a
            # label ("Agosto" twice in 2020) cannot overwrite each other.
            out.append((f"{year}__{i:02d}__{stem}", url))
    return out


def download(session: requests.Session, name: str, url: str) -> bool:
    dest = RS_INPUT / name
    if is_intact(dest):
        return True
    tmp = dest.with_suffix(dest.suffix + ".part")
    with session.get(url, stream=True, timeout=900) as r:
        r.raise_for_status()
        with tmp.open("wb") as fh:
            for chunk in r.iter_content(CHUNK):
                fh.write(chunk)
    if not is_intact(tmp):
        tmp.unlink(missing_ok=True)
        return False
    tmp.replace(dest)
    return True


def main(retries: int = 3, years: set[int] | None = None) -> None:
    RS_INPUT.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    session.headers.update({"User-Agent": BROWSER_UA})

    wanted = planned(session, years)
    print(f"{len(wanted)} monthly archives planned -> {RS_INPUT}")

    pending = dict(wanted)
    for attempt in range(1, retries + 1):
        failed: dict[str, str] = {}
        for name, url in sorted(pending.items()):
            ok = False
            try:
                ok = download(session, name, url)
            except requests.RequestException as exc:
                print(f"  {name}: {type(exc).__name__}")
            if ok:
                continue
            failed[name] = url
            print(f"  {name}: incomplete, will retry", flush=True)
        if not failed:
            break
        pending = failed
        print(f"attempt {attempt}: {len(failed)} to retry")
    else:
        # Loud and non-zero. A resumable harvest that reports success with a month
        # missing turns a transient network error into permanent silent loss.
        print(f"STILL FAILING after {retries} attempts: {sorted(pending)}")
        raise SystemExit(1)

    archives = sorted(RS_INPUT.glob("*.zip"))
    total = sum(p.stat().st_size for p in archives)
    print(f"OK: {len(archives)} archives, {total / 1e9:.2f} GB")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--retries", type=int, default=3)
    ap.add_argument("--year", type=int, action="append", help="repeatable")
    args = ap.parse_args()
    main(retries=args.retries, years=set(args.year) if args.year else None)
