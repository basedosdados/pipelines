"""Shared paths, constants and helpers for the us_ffiec_bank_reporting onboarding.

Scratch data lives outside the repo and outside Dropbox (see
.claude/rules/onboarding-workflow.md). Override with FFIEC_DATA_DIR.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

DATASET_ID = "us_ffiec_bank_reporting"

DATA_DIR = Path(
    os.environ.get(
        "FFIEC_DATA_DIR", Path.home() / "Downloads" / f"{DATASET_ID}_data"
    )
)
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"
CODE_DIR = Path(__file__).resolve().parent
ARCH_DIR = CODE_DIR / "architecture"

# --- Source endpoints -------------------------------------------------------

CDR_BULK_URL = "https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx"
CDR_PRODUCT = "ReportingSeriesSinglePeriod"

MDRM_ZIP_URL = "https://www.federalreserve.gov/apps/mdrm/pdf/MDRM.zip"

# FR Y-9C consolidated financials for holding companies, spliced across two hosts.
CHICAGOFED_BHCF_URL = (
    "https://www.chicagofed.org/~/media/others/banking/"
    "financial-institution-reports/bhc-data/bhcf{yymm}.csv"
)
NPW_FIN_PAGE = "https://www.ffiec.gov/npw/FinancialReport/FinancialDataDownload?selectedyear={year}"
NPW_BHCF_URL = "https://www.ffiec.gov/npw/FinancialReport/ReturnBHCFZipFiles?zipfilename={name}"

CRA_FLAT_URL = (
    "https://www.ffiec.gov/sites/default/files/data/cra/flat-files/{name}"
)

# www.ffiec.gov sits behind a JS/cookie WAF that returns a CAPTCHA page to
# requests/curl/wget. curl_cffi impersonating Chrome is admitted (same fix
# us_dol_oflc uses for www.dol.gov).
IMPERSONATE = "chrome124"
BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0 Safari/537.36"
)

# --- Coverage ---------------------------------------------------------------

# CDR advertises 102 periods but 30 return 0-byte zips. Real, gap-free data
# starts at 2009Q3; the four usable earlier quarters (2007Q1, 2008Q1, 2008Q4,
# 2009Q1) are deliberately excluded to keep a balanced panel.
CALL_FIRST = (2009, 3)
CALL_LAST = (2026, 2)

# Holding company financials: Chicago Fed archive 1986Q3-1999Q4, then FFIEC NPW
# 2000Q1 onward. The Chicago Fed archive now stops at 2021Q1; NPW is
# authoritative from 2000 on and is the only source used past 1999.
#
# The BHCF files bundle TWO forms. FR Y-9C is the quarterly consolidated report
# (item codes BHCK, BHCA, BHDM...), filed by ~350-450 large holding companies.
# FR Y-9SP is the SEMIANNUAL small-parent report (item codes BHSP), filed by
# ~3,700 more in June and December only. So a Q2 or Q4 file carries ~4,200 rows
# and a Q1 or Q3 file ~420. That tenfold swing is the reporting calendar, not a
# truncated download -- verified on 2020Q2, where 350 filers carry BHCK items
# and 3,740 carry BHSP items.
BHC_FIRST = (1986, 3)
BHC_LAST = (2026, 2)
BHC_CHICAGOFED_LAST_YEAR = 1999

CRA_FIRST_YEAR = 1996
CRA_LAST_YEAR = 2024

QUARTER_END_MONTH = {1: 3, 2: 6, 3: 9, 4: 12}
QUARTER_END_DAY = {1: 31, 2: 30, 3: 30, 4: 31}


def quarters(
    first: tuple[int, int], last: tuple[int, int]
) -> list[tuple[int, int]]:
    """Inclusive list of (year, quarter) between two endpoints."""
    out = []
    y, q = first
    while (y, q) <= last:
        out.append((y, q))
        q += 1
        if q == 5:
            y, q = y + 1, 1
    return out


def report_date(year: int, quarter: int) -> str:
    return f"{year:04d}-{QUARTER_END_MONTH[quarter]:02d}-{QUARTER_END_DAY[quarter]:02d}"


def mmddyyyy(year: int, quarter: int) -> str:
    return f"{QUARTER_END_MONTH[quarter]:02d}/{QUARTER_END_DAY[quarter]:02d}/{year:04d}"


def yyyymmdd(year: int, quarter: int) -> str:
    return f"{year:04d}{QUARTER_END_MONTH[quarter]:02d}{QUARTER_END_DAY[quarter]:02d}"


def quarter_of_month(month: int) -> int:
    return {3: 1, 6: 2, 9: 3, 12: 4}[month]


# --- MDRM helpers -----------------------------------------------------------

# MDRM ItemType -> the measurement_unit recorded on mdrm_item. Dollar items are
# reported in thousands at source and multiplied by 1,000 at clean time, so the
# recorded unit is plain USD. Verified against JPMorgan total assets, not assumed.
ITEM_TYPE_UNIT = {
    "F": "USD",  # Financial / reported by the filer
    "D": "USD",  # Derived from other stored variables
    "J": "USD",  # Projected
    "P": "percent",  # Stored as a percentage value (28% -> 28)
    "R": "ratio",  # Stored as a decimal (28% -> .28)
    "S": "",  # Structure -- describes the institution, not a quantity
    "E": "",  # Examination / supervision
}

# Item types whose values are genuine numeric quantities. Everything else is
# excluded from the long fact tables and logged in the crosswalk.
NUMERIC_ITEM_TYPES = {"F", "D", "J", "P", "R"}

# MDRM labels every TEXT#### item as ItemType "F", but the values are free-text
# descriptions the filer supplies for itemised "other" lines -- the paired
# RCON/RCFD item of the same number carries the amount. Casting them to FLOAT64
# would silently null every one of them, so they are excluded by prefix.
TEXT_ITEM_RE = re.compile(r"^TE(XT|\d\d)")


def is_numeric_item(item_code: str, item_type: str | None) -> bool:
    """Whether an MDRM item belongs in a FLOAT64 long fact table."""
    if TEXT_ITEM_RE.match(item_code):
        return False
    if item_code.startswith("RSSD"):
        # Structure fields (legal name, city, state, zip) -- carried by the
        # institution roster instead, where they are typed as strings.
        return False
    return (item_type or "") in NUMERIC_ITEM_TYPES


def ensure_dirs() -> None:
    for d in (INPUT_DIR, OUTPUT_DIR):
        d.mkdir(parents=True, exist_ok=True)
