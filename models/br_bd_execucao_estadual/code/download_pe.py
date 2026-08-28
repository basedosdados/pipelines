"""Download the Pernambuco source files from dados.pe.gov.br.

PE publishes one flat CSV per exercise off e-Fisco, 2008-2026, for both detailed expenses
and payments. The CKAN resource *names* are unreliable (several read "Despesas Detalhadas
2025" while pointing at a differently dated file, and the current year's resource is
re-dated on every refresh), so the year is taken from the file name in the URL and, later,
verified against the data.

Both the CSV and JSON renditions of the same year are published. Only the CSV is taken:
the JSON is 40-70% larger for identical content.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from constants import (
    BROWSER_UA,
    INPUT_DIR,
    PE_CKAN,
    PE_FIRST_YEAR,
    PE_LAST_YEAR,
    PE_PACKAGES,
)

PE_INPUT = INPUT_DIR / "pe"
CHUNK = 1 << 20

# The year can appear after the kind or before it -- PE has renamed its files at least
# once and the old convention survives for the oldest exercises:
#     despesas_detalhadas_2023_20231230.csv   -> 2023
#     pagamentos_2016_30042019.csv            -> 2016
#     2009-base-despesa.csv                   -> 2009   (year FIRST)
# A pattern assuming kind-then-year silently skipped 2009 and 2010, losing two of
# nineteen exercises with nothing but a log line to show for it. Match a four-digit year
# anywhere in the name instead, and let the caller assert the expected span.
YEAR_RE = re.compile(r"(?<!\d)(?P<year>(?:19|20)\d{2})(?!\d)")


def plan(session: requests.Session) -> dict[str, str]:
    """Map local file name -> URL, one per (kind, year), CSV only."""
    wanted: dict[str, str] = {}
    for package in PE_PACKAGES:
        r = session.get(PE_CKAN, params={"id": package}, timeout=180)
        r.raise_for_status()
        for res in r.json()["result"]["resources"]:
            url = res["url"]
            name = url.rsplit("/", 1)[-1]
            if not name.lower().endswith(".csv"):
                continue
            # The first plausible exercise year in the name wins; trailing extraction
            # stamps like "_20231230" are excluded by the range check below.
            years = [
                int(m.group("year"))
                for m in YEAR_RE.finditer(name)
                if PE_FIRST_YEAR <= int(m.group("year")) <= PE_LAST_YEAR
            ]
            if not years:
                print(f"  [skip] no exercise year in name: {name}")
                continue
            year = years[0]
            if not PE_FIRST_YEAR <= year <= PE_LAST_YEAR:
                continue
            kind = "pagamento" if "pagamento" in name.lower() else "despesa"
            wanted[f"{kind}_{year}.csv"] = url
    return wanted


def download(session: requests.Session, name: str, url: str) -> int:
    dest = PE_INPUT / name
    if dest.exists() and dest.stat().st_size > 0:
        return dest.stat().st_size
    tmp = dest.with_suffix(".part")
    with session.get(url, stream=True, timeout=1800) as r:
        r.raise_for_status()
        expected = int(r.headers.get("Content-Length") or 0)
        written = 0
        with open(tmp, "wb") as fh:
            for chunk in r.iter_content(CHUNK):
                fh.write(chunk)
                written += len(chunk)
    # A short read is the failure mode that matters here: the portal truncates large
    # transfers, and a partial CSV parses fine while silently losing the tail.
    if expected and written != expected:
        tmp.unlink(missing_ok=True)
        raise OSError(f"{name}: got {written} bytes, expected {expected}")
    tmp.replace(dest)
    return written


def main(retries: int = 3) -> None:
    PE_INPUT.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    session.headers.update({"User-Agent": BROWSER_UA})

    pending = plan(session)
    print(f"{len(pending)} files")

    # A missing exercise must be loud. The download that first ran this dropped 2009 and
    # 2010 because their files are named year-first, and the only evidence was one
    # skipped-name line in a long log.
    for kind in ("despesa", "pagamento"):
        have = {
            int(n.split("_")[1].split(".")[0])
            for n in pending
            if n.startswith(kind)
        }
        gaps = [
            y for y in range(PE_FIRST_YEAR, PE_LAST_YEAR + 1) if y not in have
        ]
        if gaps:
            print(f"  WARNING {kind}: no file found for {gaps}")
    for attempt in range(1, retries + 1):
        failed = {}
        for name, url in sorted(pending.items()):
            try:
                size = download(session, name, url)
                print(f"  {name}: {size / 1e6:.0f} MB", flush=True)
            except (requests.RequestException, OSError) as exc:
                print(f"  {name}: {exc}")
                failed[name] = url
        if not failed:
            break
        pending = failed
        print(f"attempt {attempt}: {len(failed)} to retry")
    else:
        print(f"STILL FAILING: {sorted(pending)}")
        raise SystemExit(1)
    print("PE download complete")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--retries", type=int, default=3)
    main(**vars(ap.parse_args()))
