"""Download the raw sources for au_doe_higher_education_finances.

Two publishers' pages, three retrieval paths:

1. Finance Publication 2018-2024 — live on education.gov.au. The site times out
   for plain library user agents, so every request carries a browser UA.
2. Finance Publication 2008-2017 — the per-year pages 404 on the live site and
   their download endpoints return 403, so these are recovered from the Wayback
   Machine. Files are pulled through the `id_` raw path to get the original
   bytes rather than a rewritten archive page.
3. HERDC research income time series — a single live xlsx covering 1992-2024.

Every downloaded file is verified to be a real workbook (ZIP magic) or CSV before
it is kept, and the run writes a JSON report naming exactly which years landed
and which did not.
"""

from __future__ import annotations

import html
import json
import os
import pathlib
import re
import subprocess
import time

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0 Safari/537.36"
)

DATA_DIR = pathlib.Path(
    os.environ.get(
        "AU_DOE_HEF_DATA",
        pathlib.Path.home()
        / "Downloads/au_doe_higher_education_finances_data",
    )
)
INPUT_DIR = DATA_DIR / "input"
FINANCE_DIR = INPUT_DIR / "finance"

SITE = "https://www.education.gov.au"
RESOURCES = f"{SITE}/higher-education-publications/resources"

# Live finance table downloads, keyed by year. The department mints a fresh
# numeric download id per release, so these cannot be templated.
LIVE_FINANCE = {
    2018: f"{SITE}/download/9022/finance-2018-financial-reports-higher-education-providers/7408/2018-higher-education-providers-finance-tables/xlsx",
    2019: f"{SITE}/download/12753/finance-2019-financial-reports-higher-education-providers/12739/2019-higher-education-providers-finance-tables/xlsx",
    2020: f"{SITE}/download/15001/finance-2020-financial-reports-higher-education-providers/31448/2020-higher-education-providers-finance-tables/xlsx",
    2021: f"{SITE}/download/15002/finance-2021-financial-reports-higher-education-providers/31456/2021-higher-education-providers-finance-tables/xlsx",
    2022: f"{SITE}/download/17857/finance-2022-financial-reports-higher-education-providers/37442/2022-higher-education-providers-finance-tables/xlsx",
    2023: f"{SITE}/download/18872/finance-2023-financial-reports-higher-education-providers/40111/2023-higher-education-providers-finance-tables/xlsx",
    2024: f"{SITE}/download/19832/finance-2024-financial-reports-higher-education-providers/43011/2024-higher-education-providers-finance-tables/xlsx",
}

# Per-year resource pages for the years that only survive in the archive. The
# slug pattern changed twice, so they are listed rather than generated.
#
# 2010 and 2012 cannot be recovered. Their pages are archived and both name a
# spreadsheet, but the Wayback Machine never captured the file itself: a CDX
# query for download/1484/* (2010) returns the publication's docx and pdf and
# no xls, and fetching the spreadsheet returns 404 from every capture. The
# numbers exist only inside those two documents. Everything from 2008 to 2024
# other than these two years is available.
ARCHIVED_FINANCE = {
    2017: "finance-publication-2017",
    2016: "finance-publication-2016",
    2015: "finance-publication-2015",
    2014: "finance-publication-2014",
    2013: "finance-2013-financial-reports-higher-education-providers",
    2012: "finance-2012-financial-reports-higher-education-providers",
    2011: "2011-financial-reports-higher-education-providers",
    2010: "2010-financial-reports-higher-education-providers",
    2009: "2009-financial-reports-higher-education-providers",
    2008: "2008-financial-reports-higher-education-providers",
}

RESEARCH_INCOME = (
    f"{SITE}/download/3974/research-and-development-income-time-series"
    "/43586/research-income-time-seris/xlsx"
)


def curl(
    url: str, dest: pathlib.Path | None = None, timeout: int = 180
) -> str:
    """Fetch a URL. Returns the HTTP status when writing to disk, else the body."""
    cmd = [
        "/usr/bin/curl",
        "-sL",
        "--compressed",
        "--retry",
        "4",
        "--retry-delay",
        "3",
        "--max-time",
        str(timeout),
        "-A",
        UA,
        url,
    ]
    if dest is not None:
        cmd += ["-o", str(dest), "-w", "%{http_code}"]
        return subprocess.run(
            cmd, capture_output=True, text=True
        ).stdout.strip()
    return subprocess.run(cmd, capture_output=True).stdout.decode(
        "utf-8", "replace"
    )


# An xlsx is a ZIP; a legacy xls is an OLE2 compound file. The releases up to
# 2016 ship the legacy format, so checking only for the ZIP magic throws every
# one of them away and reports the year as unarchived.
XLSX_MAGIC = b"PK"
XLS_MAGIC = b"\xd0\xcf\x11\xe0"


def is_workbook(path: pathlib.Path) -> bool:
    """True for a real workbook of either format. A 403 or 404 page is HTML."""
    if not path.exists() or path.stat().st_size <= 20_000:
        return False
    head = path.read_bytes()[:8]
    return head.startswith(XLSX_MAGIC) or head.startswith(XLS_MAGIC)


def workbook_path(year: int) -> pathlib.Path | None:
    """The stored workbook for a year, whichever format it arrived in."""
    for suffix in (".xlsx", ".xls"):
        candidate = FINANCE_DIR / f"finance_{year}{suffix}"
        if is_workbook(candidate):
            return candidate
    return None


def captures(url_without_scheme: str, attempts: int = 4) -> list[str]:
    """Wayback capture timestamps that returned 200, newest first.

    The CDX endpoint throttles a tight loop and answers with an empty body
    rather than an error, which reads exactly like "this page was never
    archived". Retrying with a growing pause distinguishes the two: without it
    six of the ten archived years were silently reported as missing.
    """
    for attempt in range(attempts):
        body = curl(
            "https://web.archive.org/cdx/search/cdx?url="
            f"{url_without_scheme}&output=text&fl=timestamp,statuscode&limit=20",
            timeout=90,
        )
        stamps = [
            line.split()[0]
            for line in body.splitlines()
            if line.split() and line.strip().endswith("200")
        ]
        if stamps:
            return sorted(set(stamps), reverse=True)
        time.sleep(5 * (attempt + 1))
    return []


def workbook_links(page: str) -> list[str]:
    """Spreadsheet download links on a resource page, un-rewritten back to origin."""
    found: list[str] = []
    for match in re.finditer(r'href="([^"]+)"', page):
        url = html.unescape(match.group(1))
        if not re.search(r"/download/.*/xlsx?$|\.xlsx?$", url, re.I):
            continue
        url = re.sub(r"^(?:https?://web\.archive\.org)?/web/\d+\w*/", "", url)
        if url.startswith("/"):
            url = SITE + url
        if url not in found:
            found.append(url)
    return found


def fetch_live_finance(report: dict) -> None:
    for year, url in sorted(LIVE_FINANCE.items()):
        dest = FINANCE_DIR / f"finance_{year}.xlsx"
        if is_workbook(dest):
            report[year] = {
                "source": "live",
                "status": "cached",
                "bytes": dest.stat().st_size,
            }
            print(f"{year}: cached", flush=True)
            continue
        code = curl(url, dest)
        if is_workbook(dest):
            report[year] = {
                "source": "live",
                "status": "ok",
                "url": url,
                "http": code,
                "bytes": dest.stat().st_size,
            }
        else:
            dest.unlink(missing_ok=True)
            report[year] = {
                "source": "live",
                "status": "failed",
                "url": url,
                "http": code,
            }
        print(f"{year}: {report[year]['status']}", flush=True)


def fetch_archived_finance(report: dict) -> None:
    for year, slug in sorted(ARCHIVED_FINANCE.items(), reverse=True):
        existing = workbook_path(year)
        dest = existing or FINANCE_DIR / f"finance_{year}.xlsx"
        if existing is not None:
            report[year] = {
                "source": "wayback",
                "status": "cached",
                "bytes": dest.stat().st_size,
            }
            print(f"{year}: cached", flush=True)
            continue

        entry = {"source": "wayback", "status": "no_capture", "slug": slug}
        for stamp in captures(
            f"education.gov.au/higher-education-publications/resources/{slug}"
        )[:3]:
            page = curl(
                f"https://web.archive.org/web/{stamp}/{RESOURCES}/{slug}",
                timeout=120,
            )
            links = workbook_links(page)
            if not links:
                entry["status"] = "captured_without_workbook"
                continue
            for link in links:
                suffix = ".xls" if re.search(r"xls$", link, re.I) else ".xlsx"
                dest = FINANCE_DIR / f"finance_{year}{suffix}"
                code = curl(
                    f"https://web.archive.org/web/{stamp}id_/{link}",
                    dest,
                    timeout=240,
                )
                if is_workbook(dest):
                    entry = {
                        "source": "wayback",
                        "status": "ok",
                        "url": link,
                        "capture": stamp,
                        "http": code,
                        "bytes": dest.stat().st_size,
                    }
                    break
                dest.unlink(missing_ok=True)
            if entry["status"] == "ok":
                break
            time.sleep(2)
        report[year] = entry
        print(f"{year}: {entry['status']}", flush=True)
        time.sleep(3)  # stay under the archive's rate limit between years


def fetch_research_income(report: dict) -> None:
    dest = INPUT_DIR / "research_income_time_series.xlsx"
    if is_workbook(dest):
        report["research_income"] = {
            "status": "cached",
            "bytes": dest.stat().st_size,
        }
        print("research income: cached", flush=True)
        return
    code = curl(RESEARCH_INCOME, dest, timeout=300)
    ok = is_workbook(dest)
    if not ok:
        dest.unlink(missing_ok=True)
    report["research_income"] = {
        "status": "ok" if ok else "failed",
        "url": RESEARCH_INCOME,
        "http": code,
    }
    print(
        f"research income: {report['research_income']['status']}", flush=True
    )


def main() -> None:
    FINANCE_DIR.mkdir(parents=True, exist_ok=True)
    report: dict = {}

    print("== Finance Publication (live 2018-2024)")
    fetch_live_finance(report)
    print("\n== Finance Publication (Wayback 2008-2017)")
    fetch_archived_finance(report)
    print("\n== HERDC research income time series")
    fetch_research_income(report)

    (DATA_DIR / "download_report.json").write_text(
        json.dumps(report, indent=2, default=str)
    )

    years = sorted(y for y in report if isinstance(y, int))
    got = [y for y in years if report[y]["status"] in ("ok", "cached")]
    missing = [y for y in years if report[y]["status"] not in ("ok", "cached")]
    print(f"\nFinance years recovered ({len(got)}): {got}")
    print(f"Finance years missing  ({len(missing)}): {missing}")


if __name__ == "__main__":
    main()
