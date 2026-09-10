"""Download every NSW Electoral Commission state general election artefact.

Source: the NSWEC past Virtual Tally Room, ``pastvtr.elections.nsw.gov.au``. One
site per electoral event, and the layout changed twice across the four events
covered here, so each event carries its own URL recipe.

Event  Site root   Notes
2023   SG2301      Current layout. Bulk XLSX + per-district ballot ZIP.
2019   SG1901      Same layout, different preference-file naming.
2015   SGE2015     Older static site under /SGE2015/.
2011   SGE2011     Oldest static site. No bulk file, no ballot data.

``SG1101`` and ``SG1501`` exist as stubs but every endpoint below the chamber
home page returns HTTP 500, so 2011 and 2015 are only reachable at the
``SGE2011`` / ``SGE2015`` roots discovered from the NSWEC results index.

The host rejects non-browser user agents with HTTP 403, so a browser UA is
mandatory on every request.

Output lands under ``$NSWEC_DATA_DIR`` (default
``~/Downloads/au_nsw_nswec_elections_data``), never inside the repository.
"""

from __future__ import annotations

import os
import pathlib
import re
import sys
import time
import urllib.parse

import requests

DATA_DIR = pathlib.Path(
    os.environ.get(
        "NSWEC_DATA_DIR",
        str(pathlib.Path.home() / "Downloads" / "au_nsw_nswec_elections_data"),
    )
)
INPUT = DATA_DIR / "input"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
BASE = "https://pastvtr.elections.nsw.gov.au"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT, "Accept": "*/*"})


def _pref_2023(slug: str, name: str) -> str:
    return f"{BASE}/SG2301/LA/{slug}/" + urllib.parse.quote(
        f"SG2301 LA Pref Data {name}.zip"
    )


def _pref_2019(slug: str, name: str) -> str:
    return f"{BASE}/SG1901/LA/{slug}/" + urllib.parse.quote(
        f"SGE2019 LA Pref Data {name}.zip"
    )


def _pref_2015(slug: str, name: str) -> str:
    return f"{BASE}/SGE2015/data/la/" + urllib.parse.quote(
        f"SGE2015 LA Pref Data_NA_{name}.zip"
    )


EVENTS: dict[int, dict] = {
    2023: {
        "code": "SG2301",
        "la_index": f"{BASE}/SG2301/LA/results",
        "lc_index": f"{BASE}/SG2301/LC/results",
        "la_xlsx": f"{BASE}/SG2301/LA/state/SGE%202023%20LA%20Final%20Votes.xlsx",
        "lc_xlsx": f"{BASE}/SG2301/LC/state/SGE%202023%20LC%20Final%20Votes.xlsx",
        "pref": _pref_2023,
        "fp_page": lambda s: f"{BASE}/SG2301/LA/{s}/cc/fp_summary",
        "dop_page": lambda s: f"{BASE}/SG2301/LA/{s}/dop/dop",
        "tcp_page": lambda s: f"{BASE}/SG2301/LA/{s}/TCP",
        "turnout_page": f"{BASE}/SG2301/LA/state/turnout",
        "elected_page": f"{BASE}/SG2301/LA/state/elected",
        "lc_elected_page": f"{BASE}/SG2301/LC/state/candidates_elected",
        "lc_fp_page": f"{BASE}/SG2301/LC/state/cc/fp_summary",
    },
    2019: {
        "code": "SG1901",
        "la_index": f"{BASE}/SG1901/LA/results",
        "lc_index": f"{BASE}/SG1901/LC/results",
        "la_xlsx": f"{BASE}/SG1901/LA/state/SGE%202019%20LA%20Final%20Votes.xlsx",
        "lc_xlsx": f"{BASE}/SG1901/LC/state/SGE%202019%20LC%20Final%20Votes.xlsx",
        "pref": _pref_2019,
        "fp_page": lambda s: f"{BASE}/SG1901/LA/{s}/cc/fp_summary",
        "dop_page": lambda s: f"{BASE}/SG1901/LA/{s}/dop/dop",
        "tcp_page": lambda s: f"{BASE}/SG1901/LA/{s}/TCP",
        "turnout_page": f"{BASE}/SG1901/LA/state/turnout",
        "elected_page": f"{BASE}/SG1901/LA/state/elected",
        "lc_elected_page": f"{BASE}/SG1901/LC/state/candidates_elected",
        "lc_fp_page": f"{BASE}/SG1901/LC/state/cc/fp_summary",
    },
    2015: {
        "code": "SGE2015",
        "la_index": f"{BASE}/SGE2015/la-home.htm",
        "lc_index": f"{BASE}/SGE2015/lc-home.htm",
        "la_xlsx": f"{BASE}/SGE2015/data/la/state/SGE%202015%20LA%20Final%20Votes.xlsx",
        "lc_xlsx": f"{BASE}/SGE2015/data/lc/state/SGE%202015%20LC%20Final%20Votes.xlsx",
        "pref": _pref_2015,
        "fp_page": lambda s: f"{BASE}/SGE2015/la/{s}/cc/fp_summary/index.htm",
        "dop_page": lambda s: f"{BASE}/SGE2015/la/{s}/dop/dop/index.htm",
        "tcp_page": lambda s: f"{BASE}/SGE2015/la/{s}/tcp/tool/index.htm",
        "turnout_page": f"{BASE}/SGE2015/la/state/turnout/index.htm",
        "elected_page": f"{BASE}/SGE2015/la/state/elected/index.htm",
        "lc_elected_page": f"{BASE}/SGE2015/lc/state/candidates_elected/index.htm",
        "lc_fp_page": f"{BASE}/SGE2015/lc/state/cc/fp_summary/index.htm",
    },
    2011: {
        "code": "SGE2011",
        "la_index": f"{BASE}/SGE2011/la_landing.htm",
        "lc_index": f"{BASE}/SGE2011/lc_landing.htm",
        # 2011 publishes the Legislative Council as standalone pages rather than
        # the per-chamber tree the later events use.
        "lc_summary": f"{BASE}/SGE2011/lc_summary.htm",
        "lc_finalcount": f"{BASE}/SGE2011/lc_finalcount.htm",
        "la_xlsx": None,
        "lc_xlsx": None,
        "pref": None,
        "fp_page": lambda s: f"{BASE}/SGE2011/la/la_district_summary-{s}.htm",
        "dop_page": None,
        "tcp_page": None,
        "turnout_page": None,
        "elected_page": None,
        "lc_elected_page": None,
        "lc_fp_page": None,
    },
}


def log(msg: str) -> None:
    print(msg, flush=True)


def fetch(url: str, dest: pathlib.Path, tries: int = 4) -> tuple[str, int]:
    """Download ``url`` to ``dest``; return ``(status, bytes)``. Idempotent."""
    if dest.exists() and dest.stat().st_size > 0:
        return "cached", dest.stat().st_size
    dest.parent.mkdir(parents=True, exist_ok=True)
    last = "fail"
    for attempt in range(tries):
        try:
            response = SESSION.get(url, timeout=180, stream=True)
            if response.status_code != 200:
                last = f"HTTP {response.status_code}"
                if response.status_code in (404, 500):
                    return last, 0
                time.sleep(2 * (attempt + 1))
                continue
            partial = dest.with_name(dest.name + ".part")
            written = 0
            with open(partial, "wb") as handle:
                for chunk in response.iter_content(1 << 20):
                    handle.write(chunk)
                    written += len(chunk)
            partial.rename(dest)
            return "ok", written
        except Exception as exc:
            last = f"ERR {exc}"
            time.sleep(3 * (attempt + 1))
    return last, 0


def district_slugs(year: int, html: str) -> list[str]:
    code = EVENTS[year]["code"]
    if year in (2023, 2019):
        found = re.findall(rf"/{code}/LA/([a-z0-9-]+)/TCP", html, re.I)
    elif year == 2015:
        found = [
            m.lower()
            for m in re.findall(
                r"/SGE2015/la/([a-z0-9-]+)/cc/fp_summary/index\.htm",
                html,
                re.I,
            )
        ]
    else:
        found = re.findall(r"la/la_district_summary-([A-Za-z_'-]+)\.htm", html)
    return sorted({s for s in found if s.lower() not in ("state", "status")})


def display_name(slug: str) -> str:
    return " ".join(
        part[:1].upper() + part[1:]
        for part in slug.replace("_", " ").split("-")
    )


def download_event(year: int) -> None:
    event = EVENTS[year]
    ydir = INPUT / str(year)
    ydir.mkdir(parents=True, exist_ok=True)
    log(f"=== {year} ({event['code']}) ===")

    for key in ("la_xlsx", "lc_xlsx"):
        if event[key]:
            status, size = fetch(event[key], ydir / f"{key}.xlsx")
            log(f"  {key}: {status} {size:,}")

    for key in (
        "la_index",
        "lc_index",
        "turnout_page",
        "elected_page",
        "lc_elected_page",
        "lc_fp_page",
        "lc_summary",
        "lc_finalcount",
    ):
        if event.get(key):
            status, size = fetch(event[key], ydir / f"{key}.html")
            log(f"  {key}: {status} {size:,}")

    index_html = (ydir / "la_index.html").read_text(
        encoding="utf-8", errors="replace"
    )
    slugs = district_slugs(year, index_html)
    (ydir / "districts.txt").write_text("\n".join(slugs), encoding="utf-8")
    log(f"  districts: {len(slugs)}")

    for i, slug in enumerate(slugs, 1):
        for key, sub in (
            ("fp_page", "fp_summary"),
            ("dop_page", "dop"),
            ("tcp_page", "tcp"),
        ):
            maker = event.get(key)
            if maker is None:
                continue
            status, _ = fetch(maker(slug), ydir / sub / f"{slug}.html")
            if status not in ("ok", "cached"):
                log(f"    [{i}/{len(slugs)}] {sub} {slug}: {status}")

        if event["pref"] is None:
            continue
        name = display_name(slug)
        status, size = fetch(
            event["pref"](slug, name), ydir / "pref" / f"{slug}.zip"
        )
        if status not in ("ok", "cached"):
            for alt in (
                name.upper(),
                name.replace(" ", "_"),
                name.replace(" ", ""),
            ):
                status, size = fetch(
                    event["pref"](slug, alt), ydir / "pref" / f"{slug}.zip"
                )
                if status in ("ok", "cached"):
                    break
        log(f"    [{i}/{len(slugs)}] pref {year} {slug}: {status} {size:,}")


def main(argv: list[str]) -> int:
    years = [int(a) for a in argv[1:]] or [2023, 2019, 2015, 2011]
    INPUT.mkdir(parents=True, exist_ok=True)
    for year in years:
        download_event(year)
    log("DONE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
