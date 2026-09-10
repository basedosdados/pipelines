"""Build the au_tas_tec_elections source manifest by scraping the TEC website.

Nothing here constructs a URL. The TEC moved its download path three times across
the covered period (``StateElection<YYYY>/results/assets/`` →
``elections-2024/results/pdf/`` → ``elections-2025/results/downloads/``), and the
per-division count exports embed a per-division count number in the file name
("Export 80", "Export Count 90"). Every artefact is therefore discovered from a
rendered page.

Two traps this module exists to handle:

1. **The results tables are not in the page.** The TEC injects them client-side with
   a hand-rolled ``w3-include-html`` routine, so a scraper that reads the division
   page sees an empty shell and concludes the archived results were deleted. The
   payloads are sibling fragments named ``fp-<division>-m.html`` and
   ``dist-<division>-m.html``. Elections up to 2018 predate that routine and carry
   their tables inline instead — both shapes must be handled.
2. **Division lists are incomplete on the year index.** Several year pages link only
   the division whose count was still running. Every division page cross-links its
   siblings, so the sibling links are followed to close the set.
"""

from __future__ import annotations

import re
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field

from pipelines.datasets.au_tas_tec_elections.constants import constants

UA = constants.USER_AGENT.value
BASE = constants.BASE_URL.value

DIV_RE = re.compile(r"results/([a-z][a-z_\-]{2,})/index\.html", re.I)


def fetch(url: str) -> tuple[int, bytes]:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=45) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, b""
    except Exception:
        return 0, b""


def fetch_text(url: str) -> str:
    """Fetch a page, raising on anything that is not a 200.

    A silent failure here is the expensive kind: a 404 body written under the data
    file's name is indistinguishable from real data once an ``if not exists`` guard
    skips it on every later run.
    """
    status, body = fetch(url)
    if status != 200:
        raise RuntimeError(f"HTTP {status} for {url}")
    return body.decode("utf-8", "replace")


def hrefs(html: str, page_url: str) -> list[str]:
    out = []
    for h in re.findall(r'href=["\']([^"\']+)["\']', html, flags=re.I):
        if h.startswith(("mailto:", "#", "javascript:", "tel:")):
            continue
        out.append(urllib.parse.urljoin(page_url, h.split("#")[0]))
    return out


@dataclass
class Contest:
    """One division's results within one electoral event."""

    division: str
    page: str
    fp_fragment: str = ""
    dist_fragment: str = ""
    inline: bool = False
    documents: list[str] = field(default_factory=list)


def discover_contests(index_url: str) -> dict[str, Contest]:
    """Find every division results page reachable from a year index, then probe it.

    Sibling links are followed because a year index frequently lists only one
    division; the division pages cross-link the rest.
    """
    found: dict[str, Contest] = {}
    pending = [index_url]
    seen_pages: set[str] = set()

    while pending:
        page = pending.pop()
        if page in seen_pages:
            continue
        seen_pages.add(page)
        status, body = fetch(page)
        if status != 200:
            continue
        html = body.decode("utf-8", "replace")
        for link in hrefs(html, page):
            m = DIV_RE.search(link)
            if not m:
                continue
            div = m.group(1).lower()
            if div in found:
                continue
            found[div] = probe_contest(div, link)
            pending.append(link)
    return found


def probe_contest(division: str, page_url: str) -> Contest:
    c = Contest(division=division, page=page_url)
    status, body = fetch(page_url)
    html = body.decode("utf-8", "replace") if status == 200 else ""
    c.documents = sorted(
        {
            urllib.parse.urljoin(page_url, x)
            for x in re.findall(
                r'href=["\']([^"\']+\.(?:xlsx?|csv))["\']', html, flags=re.I
            )
        }
    )
    for kind, attr in (("fp", "fp_fragment"), ("dist", "dist_fragment")):
        url = urllib.parse.urljoin(page_url, f"{kind}-{division}-m.html")
        st, b = fetch(url)
        if st == 200 and len(b) > 500:
            setattr(c, attr, url)
    # Elections up to 2018 carry their tables inline instead of in a fragment.
    if not c.fp_fragment and not c.dist_fragment:
        c.inline = bool(re.search(r"Distribution of preferences", html, re.I))
    return c
