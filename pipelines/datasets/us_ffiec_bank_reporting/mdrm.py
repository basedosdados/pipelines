"""The MDRM data dictionary: load it, and classify every item's unit.

The MDRM (Micro Data Reference Manual) is the Federal Reserve's authoritative
catalogue of every item code collected on every regulatory report. It is the
only acceptable source of item names here -- nothing is hand-named.

The one thing MDRM does NOT give directly is the unit. `ItemType` separates
rates and percentages from "financial" items, but lumps dollar amounts and
counts together under F. Call Report and FR Y-9C dollar amounts are reported in
THOUSANDS, counts are not, so getting this wrong scales a value by 1,000 in
silence. The classification below is therefore checked against the already
published us_fdic_bankfind tables in validate.py rather than trusted.

Lessons carried over from the us_fdic_bankfind onboarding:
  * match count words against the item NAME only, never the description
  * "offices" and "branches" are not count words -- they appear in the text of
    deposit items whose values are dollars
  * a trailing "N" in the code means nothing; do not infer from the code
"""

from __future__ import annotations

import csv
import re
import zipfile
from pathlib import Path

from pipelines.datasets.us_ffiec_bank_reporting.common import (
    INPUT_DIR,
    ITEM_TYPE_UNIT,
    is_numeric_item,
)

MDRM_ZIP = INPUT_DIR / "mdrm" / "MDRM.zip"

# Words that, in an item NAME, mark the value as a count of things rather than
# an amount of money. Word-boundary anchored: "ratio" must not fire inside
# "Operations", and "no." must not fire inside "Notes".
COUNT_RE = re.compile(
    r"(?:^|[^A-Za-z])(?:NUMBER|NUM|COUNT|NO\.)(?:[^A-Za-z]|$)", re.I
)
# Percentage / ratio wording, used only to catch items MDRM left typed as F.
RATIO_RE = re.compile(
    r"(?:^|[^A-Za-z])(?:RATIO|PERCENTAGE|PERCENT)(?:[^A-Za-z]|$)", re.I
)

# Items that carry no unit at all: yes/no questions, indicator flags, dates and
# identifiers reported as numbers.
#
# These are matched by ANCHORED rules, never by a bare substring. A first pass
# that looked for "DATE", "YEAR" or "MONTH" anywhere in the name stripped the
# USD unit from 273 genuine dollar amounts, because Call Report line items are
# routinely named "FAIR VALUE AT ACQUISITION DATE OF ...", "YEAR-TO-DATE
# MERCHANT CREDIT CARD SALES VOLUME", "ADVANCES WITH A REMAINING MATURITY OR
# NEXT REPRICING DATE OF ...", and "DEBT SECURITIES WITH A REMAINING MATURITY
# OF OVER THREE YEARS". Every one of those is money. Only a name that BEGINS or
# ENDS with the marker actually describes a date or a flag.
QUESTION_RE = re.compile(
    r"^\s*(?:DOES|DID|DO|IS|ARE|HAS|HAVE|IF|WAS|WERE)\b", re.I
)
FLAG_TAIL_RE = re.compile(r"\b(?:INDICATOR|IDENTIFIER|FLAG)\s*$", re.I)
# "...DURING THE CALENDAR YEAR-TO-DATE" ends in DATE but is a dollar amount,
# so a trailing "-TO-DATE" is excluded from the anchor.
DATE_ANCHOR_RE = re.compile(
    r"^\s*DATE\b|(?<![-\s]TO[-\s])\bDATE\s*(?:\([^)]*\))?\s*$", re.I
)


def classify_unit(item_code: str, item_name: str, item_type: str) -> str:
    """Return the measurement unit for one MDRM item.

    Returns one of: USD, percent, ratio, unit, or "" (no unit -- the value is a
    yes/no answer, an indicator code, a date or an identifier, not a quantity).
    Only items returning "USD" are multiplied by 1,000 at clean time, because
    only dollar amounts are reported in thousands.
    """
    name = (item_name or "").strip()
    if item_type == "P":
        return "percent"
    if item_type == "R":
        return "ratio"
    if item_type in ("S", "E"):
        return ""
    # item_type in F / D / J below -- MDRM lumps dollars, counts and flags here.
    if (
        QUESTION_RE.search(name)
        or FLAG_TAIL_RE.search(name)
        or DATE_ANCHOR_RE.search(name)
    ):
        return ""
    if RATIO_RE.search(name):
        return "percent"
    if COUNT_RE.search(name):
        return "unit"
    return ITEM_TYPE_UNIT.get(item_type, "")


def is_flag_item(item_name: str, unit: str) -> bool:
    """A yes/no or indicator item -- value is a code, never a magnitude."""
    if unit:
        return False
    name = (item_name or "").strip()
    return bool(QUESTION_RE.search(name) or FLAG_TAIL_RE.search(name))


def load_mdrm() -> dict[str, dict]:
    """Parse MDRM_CSV.csv into {item_code: {...}}.

    The file has a stray "PUBLIC" banner line before the header, embedded
    newlines and HTML entities inside Description, and one row per
    (item, reporting form, effective period). Item names are 1:1 with the
    8-character code -- verified: 0 of 75,264 codes carry two distinct names --
    so collapsing to one row per code is lossless for the name, and the
    reporting forms are collected into a single field.
    """
    with zipfile.ZipFile(MDRM_ZIP) as z:
        member = next(
            n for n in z.namelist() if n.upper().endswith("MDRM_CSV.CSV")
        )
        raw = z.read(member).decode("utf-8", errors="replace")
    lines = raw.split("\n")
    if lines[0].strip().upper() == "PUBLIC":
        raw = "\n".join(lines[1:])
    reader = csv.DictReader(raw.splitlines(True))
    items: dict[str, dict] = {}
    for row in reader:
        mnemonic = (row.get("Mnemonic") or "").strip().upper()
        number = (row.get("Item Code") or "").strip().upper()
        if not mnemonic or not number:
            continue
        code = mnemonic + number
        form = (row.get("Reporting Form") or "").strip()
        start = _date_part(row.get("Start Date"))
        end = _date_part(row.get("End Date"))
        rec = items.get(code)
        if rec is None:
            item_type = (row.get("ItemType") or "").strip().upper()
            name = _clean_text(row.get("Item Name"))
            items[code] = {
                "item_code": code,
                "mnemonic": mnemonic,
                "item_number": number,
                "name": name,
                "description": _clean_text(row.get("Description")),
                "item_type": item_type,
                "measurement_unit": classify_unit(code, name, item_type),
                "is_flag": is_flag_item(
                    name, classify_unit(code, name, item_type)
                ),
                "is_confidential": "1"
                if (row.get("Confidentiality") or "").strip().upper() == "Y"
                else "0",
                "reporting_forms": {form} if form else set(),
                "start_date": start,
                "end_date": end,
                "series_glossary": _clean_text(row.get("SeriesGlossary")),
            }
        else:
            if form:
                rec["reporting_forms"].add(form)
            if start and (not rec["start_date"] or start < rec["start_date"]):
                rec["start_date"] = start
            if end and end > rec["end_date"]:
                rec["end_date"] = end
            if not rec["description"]:
                rec["description"] = _clean_text(row.get("Description"))
    for rec in items.values():
        rec["reporting_form"] = "; ".join(sorted(rec.pop("reporting_forms")))
    return items


def _clean_text(value: str | None) -> str:
    if not value:
        return ""
    text = value.replace("&#x0D;", " ").replace("&#xA;", " ")
    text = (
        text.replace("&amp;", "&").replace("&quot;", '"').replace("&#39;", "'")
    )
    text = text.replace("&lt;", "<").replace("&gt;", ">")
    return re.sub(r"\s+", " ", text).strip()


def numeric_units(items: dict[str, dict]) -> dict[str, str]:
    """{item_code: unit} restricted to codes that belong in a FLOAT64 table."""
    return {
        code: rec["measurement_unit"]
        for code, rec in items.items()
        if is_numeric_item(code, rec["item_type"])
    }


def _date_part(value: str | None) -> str:
    """The date half of an MDRM timestamp, e.g. "1/1/1980 12:00:00 AM"."""
    return (value or "").strip().split(" ")[0]


def parse_date(value: str) -> str:
    """MDRM dates arrive as M/D/YYYY; 9999-12-31 means 'still collected'."""
    parts = _date_part(value).split("/")
    if len(parts) != 3:
        return ""
    try:
        m, d, y = (int(p) for p in parts)
    except ValueError:
        return ""
    if not (1 <= m <= 12 and 1 <= d <= 31):
        return ""
    return f"{y:04d}-{m:02d}-{d:02d}"


def mdrm_path() -> Path:
    return MDRM_ZIP
