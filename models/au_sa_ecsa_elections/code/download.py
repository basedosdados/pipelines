"""Download every Electoral Commission South Australia state election payload.

Two sources, on two different hosts.

``apim-ecsa-production.azure-api.net/results-display/``
    The open Azure API Management endpoint behind ECSA's ``result.ecsa.sa.gov.au``
    Angular app. ``ElectionDates`` lists the state electoral events; each event
    then has four payloads, keyed on the poll date. The ``*Change`` routes take a
    data version the app uses for delta polling, and ``0`` returns everything.

``ecsa.sa.gov.au/html/funding2024/`` and ``ecsa.sa.gov.au/html/fdarchive/``
    Two server-rendered PHP indexes of campaign funding disclosure returns. Only
    the filing envelope is in HTML; the itemised gifts sit inside PDF
    attachments and are not extracted here. The index of the current portal
    paginates on a non-unique key and silently omits 93 of its 924 returns, so
    every return is also fetched by id from its detail page.

Every request raises on a non-2xx status, and nothing is skipped because a file
happens to already exist: a stale or truncated artefact has to fail loudly rather
than be silently reused.

Output lands under ``$ECSA_DATA_DIR`` (default
``~/Downloads/au_sa_ecsa_elections_data``), never inside the repository.

Usage::

    PYTHONPATH=. python models/au_sa_ecsa_elections/code/download.py [api|disclosure]
"""

from __future__ import annotations

import json
import pathlib
import re
import sys
import time

import requests

from pipelines.datasets.au_sa_ecsa_elections.constants import (
    constants,
    data_dir,
)

INPUT = data_dir() / "input"
API_BASE = constants.API_BASE.value
SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": constants.USER_AGENT.value,
        "Accept": "application/json, text/html, */*",
        # The gateway is CORS-locked to the Angular app's origin. It serves the
        # payload regardless, but sending the real origin keeps the request
        # indistinguishable from the app's own.
        "Origin": "https://result.ecsa.sa.gov.au",
        "Referer": "https://result.ecsa.sa.gov.au/",
    }
)

FUNDING_PORTALS = {
    "funding2024": "https://ecsa.sa.gov.au/html/funding2024/",
    "fdarchive": "https://ecsa.sa.gov.au/html/fdarchive/",
}


def log(message: str) -> None:
    print(message, flush=True)


def fetch(url: str, attempts: int = 4) -> requests.Response:
    """GET a URL, raising on any non-2xx status.

    A 404 body written to disk under the data file's name is worse than a crash:
    it is skipped forever after and never noticed.
    """
    last: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = SESSION.get(url, timeout=180)
            response.raise_for_status()
            return response
        except requests.RequestException as error:
            last = error
            status = getattr(error.response, "status_code", None)
            if status is not None and 400 <= status < 500 and status != 429:
                raise RuntimeError(f"{url} returned HTTP {status}") from error
            log(f"    attempt {attempt}/{attempts} failed for {url}: {error}")
            time.sleep(2 * attempt)
    raise RuntimeError(f"{url} failed after {attempts} attempts") from last


def write_json(payload: object, target: pathlib.Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(payload, ensure_ascii=False), encoding="utf-8"
    )


def download_api() -> dict[str, int]:
    """Fetch ElectionDates, then the four payloads for every state election."""
    counts: dict[str, int] = {}
    dates = fetch(API_BASE + constants.ROUTE_ELECTION_DATES.value).json()
    write_json(dates, INPUT / "api" / "ElectionDates.json")
    elections = dates["elections"]
    log(f"  ElectionDates: {len(elections)} state electoral events")

    for election in elections:
        date = election["electionDate"]
        event = election["electionEvent"]
        log(f"  {event} ({date}, {election['electionStatus']})")
        for stem, route in constants.PER_ELECTION_ROUTES.value.items():
            url = API_BASE + route.format(date=date)
            response = fetch(url)
            # An event with no contest in that chamber answers HTTP 204 with an
            # empty body rather than 404 — a by-election has no Legislative
            # Council race. Record the gap; never write an empty file that a
            # later run would treat as real.
            if response.status_code == 204 or not response.content.strip():
                log(
                    f"    {stem}: ABSENT (HTTP {response.status_code}, empty body)"
                )
                counts[f"{date}/{stem}"] = 0
                continue
            target = INPUT / "api" / date / f"{stem}.json"
            write_json(response.json(), target)
            counts[f"{date}/{stem}"] = target.stat().st_size
            log(f"    {stem}: {target.stat().st_size:,} bytes")
    return counts


PAGE_ROW = re.compile(
    r'<tr[^>]*data-href="view\.php\?ID=(\d+)"[^>]*>(.*?)</tr>', re.S
)
CELL = re.compile(r"<td[^>]*>(.*?)</td>", re.S)
RECORD_COUNT = re.compile(r"([\d,]+)\s+records returned", re.I)


def download_disclosure() -> dict[str, int]:
    """Fetch every page of both campaign funding disclosure indexes.

    Only the return-level index is harvested. Gift-level detail lives inside PDF
    attachments and is out of scope, so no gift table is produced rather than a
    partial one that would look complete.
    """
    counts: dict[str, int] = {}
    for name, base in FUNDING_PORTALS.items():
        target_dir = INPUT / "disclosure" / name
        target_dir.mkdir(parents=True, exist_ok=True)
        first = fetch(base + "index.php").text
        match = RECORD_COUNT.search(first)
        if not match:
            raise RuntimeError(
                f"{base}: could not read the record count from the index page; "
                "the portal layout has changed and the harvest would silently "
                "truncate"
            )
        total = int(match.group(1).replace(",", ""))
        (target_dir / "page_001.html").write_text(first, encoding="utf-8")
        seen = len(PAGE_ROW.findall(first))
        page = 1
        log(f"  {name}: {total:,} records returned, {seen} rows on page 1")
        while seen < total:
            page += 1
            html = fetch(f"{base}index.php?page={page}").text
            rows = PAGE_ROW.findall(html)
            if not rows:
                raise RuntimeError(
                    f"{base} page {page} returned no rows while {seen} of "
                    f"{total} records had been seen; refusing to stop early"
                )
            (target_dir / f"page_{page:03d}.html").write_text(
                html, encoding="utf-8"
            )
            seen += len(rows)
            if page % 10 == 0:
                log(f"    {name}: page {page}, {seen:,}/{total:,} rows")
        if seen != total:
            raise RuntimeError(
                f"{name}: harvested {seen} rows, index promised {total}"
            )
        counts[name] = seen
        log(f"  {name}: {seen:,} rows over {page} pages")
    return counts


# A detail page for an id that does not exist still answers 200, with a short stub
# that carries no field labels. Existence is decided on content, never on status.
DETAIL_MARKER = "Date Lodged"
DETAIL_MARGIN = 60


def download_disclosure_details() -> dict[str, int]:
    """Fetch one detail page per return id, to close the index's pagination gap.

    The current portal paginates on a non-unique sort key, so consecutive pages
    overlap: 924 row instances resolve to only 831 distinct ids, and 93 returns
    are never served by the index at all. Re-sweeping does not help — the
    pagination is deterministic, so the same request returns the same rows. The
    detail pages are keyed on the id itself and therefore complete.
    """
    counts: dict[str, int] = {}
    for name, base in FUNDING_PORTALS.items():
        pages = sorted((INPUT / "disclosure" / name).glob("page_*.html"))
        seen = {
            int(rid)
            for page in pages
            for rid, _ in PAGE_ROW.findall(page.read_text(encoding="utf-8"))
        }
        if not seen:
            raise RuntimeError(
                f"{name}: no index pages harvested; run the index first"
            )
        target_dir = INPUT / "disclosure" / f"{name}_detail"
        target_dir.mkdir(parents=True, exist_ok=True)
        low = max(1, min(seen) - DETAIL_MARGIN)
        high = max(seen) + DETAIL_MARGIN
        found = 0
        log(
            f"  {name}: probing ids {low}-{high} ({len(seen)} seen in the index)"
        )
        for ident in range(low, high + 1):
            html = fetch(f"{base}view.php?ID={ident}").text
            if DETAIL_MARKER not in html:
                continue
            (target_dir / f"{ident}.html").write_text(html, encoding="utf-8")
            found += 1
            if found % 100 == 0:
                log(f"    {name}: {found} detail pages")
        counts[name] = found
        log(f"  {name}: {found} detail pages over ids {low}-{high}")
    return counts


def main(argv: list[str]) -> int:
    wanted = set(argv[1:]) or {"api", "disclosure"}
    summary: dict[str, object] = {}
    if "api" in wanted:
        log("API results")
        summary["api"] = download_api()
    if "disclosure" in wanted:
        log("Disclosure returns")
        summary["disclosure"] = download_disclosure()
        summary["disclosure_detail"] = download_disclosure_details()
    (data_dir() / "download_summary.json").write_text(
        json.dumps(summary, indent=1), encoding="utf-8"
    )
    log("DONE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
