"""Build the FDIC indicator catalog from the published data dictionaries.

The FDIC BankFind API ships two OpenAPI property files that document every field
it can return.  They are the source of truth for names, titles, descriptions and
units, and everything else in this dataset is derived from them:

    risview_properties.yaml       2,378 quarterly financial fields
    institution_properties.yaml     152 institution master fields

`classify_unit` is the load-bearing part.  FDIC reports dollar amounts in
THOUSANDS of dollars, ratios in percent, and a handful of fields are plain
counts.  Getting that wrong would silently scale a column by 1,000, so the
classification is deliberately conservative: a field is treated as a dollar
amount only when nothing marks it as a ratio or a count.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import yaml

DOCS_URL = "https://api.fdic.gov/banks/docs"

# FDIC suffix conventions, learned from the published titles:
#   ...R / ...QR  ratio (percent)
#   ...Y          "to earning assets" style ratio (percent)
#   ...Q          quarterly rather than year-to-date amount
RATIO_SUFFIX = ("R", "QR")
RATIO_WORDS = re.compile(
    r"\bRATIO\b|\bPERCENT|^%|\bRETURN ON\b|\bMARGIN\b|/|\bYIELD\b|\bRATE\b"
    r"|\bTO ASSETS\b|\bTO NET INCOME\b|\bTO EARNING ASSETS\b|\bTO TOTAL\b|\bTO GROSS\b",
    re.I,
)
# "offices"/"branches" are deliberately NOT count words: titles like "Deposits
# held in domestic offices" are dollar amounts.  A genuine count says "Number of".
# Counts are matched against the TITLE only.  Matching the description instead
# classified "Total deposits" as a count, because its description happens to say
# "domestic offices" -- which would have divided every deposit balance by 1,000.
COUNT_WORDS = re.compile(
    r"\bNUM\b|\bNUMBER\b|\bNO\.\b|\bCOUNT\b|EMPLOYEE", re.I
)
FLAG_WORDS = re.compile(r"\bFLAG\b|\bINDICATOR\b|^IS |\bCODE\b", re.I)


def load_properties(docs_dir: Path, name: str) -> dict:
    doc = yaml.safe_load((docs_dir / f"{name}.yaml").read_text())
    return doc["properties"]["data"]["properties"]


def classify_unit(code: str, prop: dict) -> str:
    """Return the measurement unit for one FDIC field.

    One of: percent, unit (a dimensionless count), USD_thousand, or "" for
    strings and flags.  Amounts are the residual category, which is why every
    positive signal for ratio/count is checked first.
    """
    if prop.get("type") == "string":
        return ""
    title = prop.get("title") or ""
    description = prop.get("description") or ""
    if code.endswith(RATIO_SUFFIX) or RATIO_WORDS.search(
        f"{title} {description}"
    ):
        return "percent"
    # Flags are matched against title AND description: "BRANCHING" reads like an
    # amount until you read its description, which calls it a flag.
    if FLAG_WORDS.search(f"{title} {description}"):
        return ""
    if COUNT_WORDS.search(title):
        return "unit"
    return "USD_thousand"


def humanize(title: str) -> str:
    """Turn an ALL-CAPS FDIC title into a readable sentence-case name."""
    if not title:
        return ""
    if title.isupper():
        title = title.title()
        # keep well-known acronyms and money shorthand upright
        for wrong, right in EXPANSIONS:
            title = re.sub(rf"\b{re.escape(wrong)}\b", right, title)
        return title.strip()
    return title.strip()


EXPANSIONS: list[tuple[str, str]] = [
    ("Re", "Real Estate"),
    ("C&I", "C&I"),
    ("Ln&Ls", "Loans & Leases"),
    ("Fdic", "FDIC"),
    ("Us", "US"),
    ("Rbc", "RBC"),
    ("Pca", "PCA"),
    ("Qbp", "QBP"),
    ("Msa", "MSA"),
    ("Cbsa", "CBSA"),
    ("Ytd", "YTD"),
    ("Dep", "Deposits"),
    ("Chg-Off", "Charge-Off"),
    ("Nonres", "Nonresidential"),
    ("Multifam", "Multifamily"),
    ("Cavg", "Average"),
    ("C&I", "Commercial and Industrial"),
    ("Prin Sec", "Principal Securitised"),
    ("Est Uninsured Dep", "Estimated Uninsured Deposits"),
    ("And", "and"),
    ("Of", "of"),
    ("To", "to"),
    ("In", "in"),
    ("For", "for"),
    ("Or", "or"),
]


def build(docs_dir: Path) -> dict[str, dict]:
    """Return {code: record} for every documented FDIC financial field."""
    risview = load_properties(docs_dir, "risview_properties")
    institutions = load_properties(docs_dir, "institution_properties")

    catalog: dict[str, dict] = {}
    for code, prop in sorted(risview.items()):
        catalog[code] = {
            "indicator_id": code,
            "name": humanize(prop.get("title") or code),
            "description": (prop.get("description") or "").strip(),
            "unit_of_measure": classify_unit(code, prop),
            "source_type": prop.get("type") or "",
            "is_ratio": "yes" if code.endswith(RATIO_SUFFIX) else "no",
            "is_quarterly": "yes"
            if code.endswith("Q") and not code.endswith("QR")
            else "no",
            "is_flag": "yes"
            if FLAG_WORDS.search(
                f"{prop.get('title') or ''} {prop.get('description') or ''}"
            )
            else "no",
            "in_institution_table": "yes" if code in institutions else "no",
        }
    return catalog


if __name__ == "__main__":
    here = Path(__file__).resolve().parent
    docs = Path.home() / "Downloads/us_fdic_bankfind_data/input/docs"
    catalog = build(docs)
    (here / "indicator_catalog.json").write_text(json.dumps(catalog, indent=0))

    units: dict[str, int] = {}
    for record in catalog.values():
        units[record["unit_of_measure"] or "(none)"] = (
            units.get(record["unit_of_measure"] or "(none)", 0) + 1
        )
    print(f"indicators: {len(catalog)}")
    for unit, n in sorted(units.items(), key=lambda kv: -kv[1]):
        print(f"  {unit:>14}  {n}")
