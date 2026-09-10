"""Enumerate every TEC results page and the artefacts it actually carries.

The TEC renders its results tables through a hand-rolled ``w3-include-html``
client-side include, so the tables are **absent** from the HTML a crawler sees. The
real payloads are separate fragments named ``fp-<division>-m.html`` (first
preferences) and ``dist-<division>-m.html`` (distribution of preferences), sitting
beside the division's ``index.html``. A scrape that reads only the rendered page
concludes, wrongly, that the archived results were deleted.

This pass walks every election section, finds the per-division results pages, and
probes both fragments plus the spreadsheet links. It writes ``results_map.json``.

Run: PYTHONPATH=. ~/.venvs/bd-pipelines-tas/bin/python \
        models/au_tas_tec_elections/code/discover_results.py
"""

from __future__ import annotations

import json
import re
import urllib.parse
import urllib.request

from pipelines.datasets.au_tas_tec_elections.constants import (
    constants,
    data_root,
)

UA = constants.USER_AGENT.value
BASE = constants.BASE_URL.value

# Section index pages that list a year's per-division results pages.
SECTION_PAGES = [
    "house-of-assembly/StateElection2014/Results/Results.html",
    "house-of-assembly/StateElection2018/Results/Results.html",
    "house-of-assembly/StateElection2021/index.html",
    "house-of-assembly/elections-2024/index.html",
    "house-of-assembly/elections-2025/index.html",
    "legislative-council/LegislativeCouncilElections_2018/index.html",
    "legislative-council/LegislativeCouncilElections_2019/index.html",
    "legislative-council/LegislativeCouncilElections_2020/index.html",
    "legislative-council/LegislativeCouncilElections_2021/index.html",
    "legislative-council/legislative-council-elections-2022/index.html",
    "legislative-council/legislative-council-byelection-2022/index.html",
    "legislative-council/elections-2023/index.html",
    "legislative-council/elections-2024/index.html",
    "legislative-council/elections-2025/index.html",
    "legislative-council/elections-2026/index.html",
    "legislative-council/Previous_Elections/LC2017/index.html",
    "legislative-council/Previous_Elections/Pembroke2017/index.html",
]


def get(url: str) -> tuple[int, bytes]:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=40) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:  # type: ignore[attr-defined]
        return e.code, b""
    except Exception:
        return 0, b""


def main() -> int:
    out: dict[str, dict] = {}
    for page in SECTION_PAGES:
        url = urllib.parse.urljoin(BASE, page)
        status, body = get(url)
        html = body.decode("utf-8", "replace")
        hrefs = re.findall(r'href=["\']([^"\']+)["\']', html, flags=re.I)
        divisions: dict[str, dict] = {}
        for h in hrefs:
            m = re.search(r"results/([a-z_\-]+)/index\.html", h, re.I)
            if not m:
                continue
            div = m.group(1).lower()
            if div in divisions or div in ("index", "assets", "pdf"):
                continue
            page_url = urllib.parse.urljoin(url, h)
            entry: dict = {"page": page_url}
            # Probe both include fragments and the spreadsheet links.
            for kind in ("fp", "dist"):
                frag = urllib.parse.urljoin(page_url, f"{kind}-{div}-m.html")
                st, b = get(frag)
                if st == 200 and len(b) > 500:
                    entry[kind] = {"url": frag, "bytes": len(b)}
            st, b = get(page_url)
            docs = [
                urllib.parse.urljoin(page_url, x)
                for x in re.findall(
                    r'href=["\']([^"\']+\.(?:xlsx?|csv|pdf))["\']',
                    b.decode("utf-8", "replace"),
                    flags=re.I,
                )
            ]
            entry["docs"] = sorted(set(docs))
            divisions[div] = entry
        out[page] = {"status": status, "divisions": divisions}
        got = sum(1 for d in divisions.values() if "fp" in d or "dist" in d)
        print(
            f"{status}  {page:64s} {len(divisions):2d} divisions, "
            f"{got} with fragments",
            flush=True,
        )

    path = data_root() / "results_map.json"
    path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"\n-> {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
