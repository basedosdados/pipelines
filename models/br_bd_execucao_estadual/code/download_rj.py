"""Download Rio de Janeiro's SEFAZ despesa series from dadosabertos.rj.gov.br.

RJ is the largest state still missing from this dataset and the thinnest source in it.
Its catalogue holds 1,119 packages, but the fiscal content is almost entirely a
per-agency PDF dump; the one real series is SEFAZ's `tfe-despesa`, ten annual CSVs of
~10 MB each, described as a D+1 mirror of SIAFE-Rio.

**This host requires a Brazilian IP.** It answers 200 from São Paulo and refuses
otherwise, so a scheduled refresh from a non-Brazilian cluster cannot work. See
SOURCE_LESSONS.md, "The Brazilian-IP requirement is a PIPELINE constraint".

Two source defects are handled here:

* **2019 and 2021 are each published twice** as separate resources. Resources are
  de-duplicated on the CKAN resource id, and the exercise is read from the file name
  rather than from the resource order, so a repeated year cannot be counted twice.
* **TLS connections drop mid-download.** Every fetch retries, and a short file is
  rejected rather than kept -- a truncated CSV passes a "file exists and is non-empty"
  check and then reads as a smaller year.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

sys.path.insert(0, str(Path(__file__).resolve().parent))
from constants import (
    BROWSER_UA,
    INPUT_DIR,
    RJ_CKAN,
    RJ_FIRST_YEAR,
    RJ_LAST_YEAR,
    RJ_PACKAGE,
    RJ_TABLE,
)

RJ_INPUT = INPUT_DIR / "rj"
YEAR = re.compile(r"(?<!\d)(20\d{2})(?!\d)")


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": BROWSER_UA})
    # The host drops connections mid-transfer; urllib3 retries the request rather than
    # leaving a short file behind.
    s.mount(
        "https://",
        HTTPAdapter(
            max_retries=Retry(
                total=6,
                backoff_factor=1.5,
                status_forcelist=[502, 503, 504],
                allowed_methods=None,
            )
        ),
    )
    return s


def resources(session: requests.Session) -> list[dict]:
    """Every CSV resource of `tfe-despesa`, de-duplicated on resource id."""
    body = session.get(RJ_CKAN, params={"id": RJ_PACKAGE}, timeout=120).json()
    out: dict[str, dict] = {}
    for r in body["result"].get("resources", []):
        if (r.get("format") or "").upper() != "CSV":
            continue
        # Keyed on the CKAN resource id, NOT the file name: 2019 and 2021 are each
        # published twice under the same name, and a name-keyed dict keeps one at
        # random while a list double-counts the year.
        out[r["id"]] = r
    return list(out.values())


def plan(items: list[dict], years: set[int]) -> list[tuple[int, dict]]:
    """Map each resource to its exercise, refusing anything ambiguous."""
    chosen: dict[int, dict] = {}
    duplicates: dict[int, int] = {}
    for r in items:
        name = r.get("name") or r["url"].rsplit("/", 1)[-1]
        found = YEAR.findall(name)
        if len(set(found)) != 1:
            raise SystemExit(
                f"{name!r}: expected exactly one year in the resource name, found "
                f"{sorted(set(found))}"
            )
        year = int(found[0])
        if year not in years:
            continue
        if year in chosen:
            duplicates[year] = duplicates.get(year, 1) + 1
            # Both copies are the same size; keep the first deterministically and say so
            # rather than letting the later one win silently.
            continue
        chosen[year] = r
    for year, n in sorted(duplicates.items()):
        print(f"  [dup] {year} published {n} times; keeping one", flush=True)
    return sorted(chosen.items())


def download(session: requests.Session, year: int, resource: dict) -> int:
    dest = RJ_INPUT / f"{RJ_TABLE}_{year}.csv"
    meta = dest.with_suffix(".json")
    RJ_INPUT.mkdir(parents=True, exist_ok=True)

    declared = int(resource.get("size") or 0)
    if (
        dest.exists()
        and meta.exists()
        and json.loads(meta.read_text()).get("bytes") == dest.stat().st_size
    ):
        print(
            f"  {year}: skip ({dest.stat().st_size / 1e6:.1f} MB)",
            flush=True,
        )
        return dest.stat().st_size

    for attempt in range(5):
        try:
            r = session.get(resource["url"], timeout=900, stream=True)
            r.raise_for_status()
            tmp = dest.with_suffix(".part")
            n = 0
            with tmp.open("wb") as fh:
                for chunk in r.iter_content(1 << 20):
                    fh.write(chunk)
                    n += len(chunk)
            r.close()
            # A dropped connection yields a short file that would otherwise pass an
            # "exists and non-empty" check and read as a smaller exercise.
            if declared and abs(n - declared) > max(4096, 0.02 * declared):
                tmp.unlink(missing_ok=True)
                print(
                    f"  {year}: got {n:,} bytes, catalogue says {declared:,} "
                    f"(attempt {attempt + 1})",
                    flush=True,
                )
                time.sleep(10 * (attempt + 1))
                continue
            tmp.replace(dest)
            meta.write_text(
                json.dumps({"year": year, "bytes": n, "url": resource["url"]})
            )
            print(f"  {year}: {n / 1e6:.1f} MB", flush=True)
            return n
        except Exception as exc:
            print(
                f"  {year}: {type(exc).__name__} (attempt {attempt + 1})",
                flush=True,
            )
            time.sleep(10 * (attempt + 1))
    raise SystemExit(f"{year}: could not be downloaded")


def main(years: set[int] | None = None) -> None:
    session = _session()
    years = years or set(range(RJ_FIRST_YEAR, RJ_LAST_YEAR + 1))
    items = resources(session)
    print(f"{len(items)} distinct CSV resource(s) in {RJ_PACKAGE}", flush=True)
    selected = plan(items, years)
    if not selected:
        raise SystemExit("no resources matched the requested years")
    total = sum(download(session, year, r) for year, r in selected)
    got = {y for y, _ in selected}
    missing = sorted(years - got)
    print(f"\n{len(selected)} exercise(s), {total / 1e6:.1f} MB")
    if missing:
        print(f"no resource published for: {missing}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, action="append")
    args = parser.parse_args()
    main(years=set(args.year) if args.year else None)
