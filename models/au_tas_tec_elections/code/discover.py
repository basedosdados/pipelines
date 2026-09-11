"""Crawl tec.tas.gov.au and inventory every downloadable results file.

The TEC moved the download path between the 2024 and 2025 House of Assembly
elections (``elections-2024/results/<div>/`` vs
``elections-2025/results/<div>/pdf/``), and the archived elections use three further
layouts. No URL in this dataset is ever constructed: every one is scraped from a
rendered page, and this script is what does the scraping.

Run: PYTHONPATH=. ~/.venvs/bd-pipelines-tas/bin/python models/au_tas_tec_elections/code/discover.py
"""

from __future__ import annotations

import json
import re
import sys
import time
import urllib.parse
import urllib.request
from collections import deque

from pipelines.datasets.au_tas_tec_elections.constants import (
    constants,
    data_root,
)

BASE = constants.BASE_URL.value
UA = constants.USER_AGENT.value
HOST = "www.tec.tas.gov.au"

DOC_RE = re.compile(r"\.(xlsx?|csv|zip|json|pdf|gif|txt)(\?|$)", re.I)
# Only these sections matter; the rest of the site is enrolment and corporate pages.
IN_SCOPE = re.compile(
    r"/(house-of-assembly|legislative-council|info/Publications)/", re.I
)
SKIP = re.compile(
    r"/(Chronology|Maps_|Redistribution|ways-to-vote|candidate-info|faqs?|"
    r"Enrolment|MyReps|learning-hub|Quarterly_Roll)",
    re.I,
)


def fetch(url: str) -> tuple[str, str] | None:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=45) as r:
            if r.status != 200:
                return None
            ctype = r.headers.get("Content-Type", "")
            if "html" not in ctype:
                return None
            return r.read().decode("utf-8", "replace"), r.geturl()
    except Exception:
        return None


def main() -> int:
    seen: set[str] = set()
    docs: dict[str, list[str]] = {}
    queue: deque[tuple[str, int]] = deque(
        (urllib.parse.urljoin(BASE, p), 0) for p in constants.CRAWL_ROOTS.value
    )
    pages_read = 0

    while queue:
        url, depth = queue.popleft()
        url = url.split("#")[0]
        if url in seen or depth > 4:
            continue
        seen.add(url)
        got = fetch(url)
        if got is None:
            continue
        html, final = got
        pages_read += 1
        if pages_read % 25 == 0:
            print(
                f"  {pages_read} pages, {len(docs)} docs, {len(queue)} queued",
                flush=True,
            )

        hrefs = re.findall(r'href=["\']([^"\']+)["\']', html, flags=re.I)
        for h in hrefs:
            if h.startswith(("mailto:", "#", "javascript:", "tel:")):
                continue
            link = urllib.parse.urljoin(final, h).split("#")[0]
            if urllib.parse.urlparse(link).netloc != HOST:
                continue
            if DOC_RE.search(link):
                docs.setdefault(link, []).append(final)
            elif (
                IN_SCOPE.search(link)
                and not SKIP.search(link)
                and link not in seen
            ):
                queue.append((link, depth + 1))
        time.sleep(0.15)

    out = data_root()
    out.mkdir(parents=True, exist_ok=True)
    path = out / "inventory.json"
    payload = {
        "pages_crawled": pages_read,
        "documents": {k: sorted(set(v)) for k, v in sorted(docs.items())},
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    spreadsheets = [
        d for d in docs if re.search(r"\.(xlsx?|csv)(\?|$)", d, re.I)
    ]
    print(
        f"\ncrawled {pages_read} pages; {len(docs)} documents "
        f"({len(spreadsheets)} spreadsheets) -> {path}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
