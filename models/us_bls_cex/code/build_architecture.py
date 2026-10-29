"""Generate the architecture CSVs for us_bls_cex.

Two layers:

* LABSTAT published tables (``series``, ``annual``): hand-written below.
* PUMD microdata (8 tables): generated from the union of the actual CSV headers
  in every selected quarter file (1996-2025 Q1), joined to the BLS
  Interview/Diary dictionary for descriptions, codes and flag names.

Naming is hybrid (agreed 2026-10-01): keys, dates and weights get English
names; every other survey variable keeps its BLS name, lowercased, so the BLS
dictionary applies directly. Flag columns keep BLS's trailing underscore.

Type follows arithmetic meaning (.claude/rules/bigquery-conventions.md): coded
variables are STRING covered by ``dicionario``; dollar amounts, ages and counts
are numeric with a unit; weights are FLOAT64 without a unit; anything the rules
below cannot classify falls back to STRING and is listed by ``--report``.
"""

import argparse
import csv
import re
from collections import defaultdict
from pathlib import Path

import pandas as pd

from pipelines.datasets.us_bls_cex.pumd_files import (
    DICTIONARY_XLSX,
    FAMILIES,
    read_header,
    selected_quarter_files,
)

OUT = Path(__file__).parent / "architecture"

HEADER = [
    "name",
    "bigquery_type",
    "description",
    "temporal_coverage",
    "covered_by_dictionary",
    "directory_column",
    "measurement_unit",
    "has_sensitive_data",
    "observations",
    "original_name",
]

YEAR_DIR = "br_bd_diretorios_data_tempo.ano:ano"
MAX_DESC = 1000

# --------------------------------------------------------------------------
# LABSTAT layer
# --------------------------------------------------------------------------


def col(
    name,
    typ,
    desc,
    *,
    dictionary="no",
    directory="",
    unit="",
    obs="",
    original="",
    coverage="",
):
    return {
        "name": name,
        "bigquery_type": typ,
        "description": desc,
        "temporal_coverage": coverage,
        "covered_by_dictionary": dictionary,
        "directory_column": directory,
        "measurement_unit": unit,
        "has_sensitive_data": "no",
        "observations": obs,
        "original_name": original or name,
    }


SERIES = [
    col("series_id", "STRING", "BLS LABSTAT series identifier"),
    col(
        "category_id",
        "STRING",
        "Top-level category of the series: expenditures, income, consumer unit characteristics or addenda",
        original="category_code",
    ),
    col(
        "category_name",
        "STRING",
        "Name of the category",
        original="category_text",
    ),
    col(
        "subcategory_id",
        "STRING",
        "Subcategory code, e.g. food, housing or income before taxes",
        original="subcategory_code",
    ),
    col(
        "subcategory_name",
        "STRING",
        "Name of the subcategory",
        original="subcategory_text",
    ),
    col(
        "item_id",
        "STRING",
        "Item code, either a 6-digit numeric code or a mnemonic such as TOTALEXP",
        original="item_code",
    ),
    col("item_name", "STRING", "Name of the item", original="item_text"),
    col(
        "item_display_level",
        "INT64",
        "Indentation level of the item in the BLS table hierarchy, from 0 (top) to 3",
        unit="level",
        obs="Ordinal position in the published table tree",
        original="display_level",
    ),
    col(
        "demographics_id",
        "STRING",
        "Demographic classification the series is cut by, e.g. LB01 all consumer units or LB02 income quintiles",
        original="demographics_code",
    ),
    col(
        "demographics_name",
        "STRING",
        "Name of the demographic classification",
        original="demographics_text",
    ),
    col(
        "characteristics_id",
        "STRING",
        "Group within the demographic classification, e.g. a specific income quintile",
        original="characteristics_code",
    ),
    col(
        "characteristics_name",
        "STRING",
        "Name of the group within the demographic classification",
        original="characteristics_text",
    ),
    col(
        "statistic",
        "STRING",
        "Statistic published in the series; M is the mean",
        dictionary="yes",
        original="process_code",
    ),
    col(
        "series_title",
        "STRING",
        "Full title of the series as published by BLS",
    ),
    col(
        "begin_year",
        "INT64",
        "First year with data in the series",
        unit="year",
    ),
    col("end_year", "INT64", "Last year with data in the series", unit="year"),
]

ANNUAL = [
    col(
        "year",
        "INT64",
        "Reference year of the estimate",
        directory=YEAR_DIR,
        unit="year",
        obs="Partition column",
    ),
    col(
        "series_id",
        "STRING",
        "BLS LABSTAT series identifier; see the series table",
    ),
    col(
        "mean",
        "FLOAT64",
        "Published estimate: mean annual expenditure or income per consumer unit, or the mean characteristic",
        unit="usd",
        obs="Unit is US dollars for expenditure and income items; consumer-unit characteristics (e.g. persons, age, percent) carry the unit named in the item",
        original="value",
    ),
    col(
        "standard_error",
        "FLOAT64",
        "Standard error of the mean",
        unit="usd",
        coverage="2010(1)2024",
        original="value (aspect_type E)",
    ),
    col(
        "relative_standard_error",
        "FLOAT64",
        "Relative standard error of the mean",
        unit="percent",
        coverage="2010(1)2024",
        original="value (aspect_type R0)",
    ),
    col(
        "expenditure_share",
        "FLOAT64",
        "Share of the item in average annual expenditures of the group",
        unit="percent",
        coverage="2010(1)2024",
        original="value (aspect_type ES)",
    ),
    col(
        "aggregate_expenditure",
        "FLOAT64",
        "Aggregate expenditure on the item for the group",
        unit="usd_million",
        obs="For characteristics other than all consumer units, BLS reports the share of the aggregate instead",
        coverage="2011(1)2024",
        original="value (aspect_type AG)",
    ),
    col(
        "aggregate_share",
        "FLOAT64",
        "Share of the aggregate expenditure on the item accounted for by the group",
        unit="percent",
        coverage="2011(1)2024",
        original="value (aspect_type AS)",
    ),
    col(
        "percent_reporting",
        "FLOAT64",
        "Percentage of consumer units in the group reporting an expenditure on the item",
        unit="percent",
        coverage="2010(1)2024",
        original="value (aspect_type RP)",
    ),
    col(
        "footnote_codes",
        "STRING",
        "Footnote codes attached to the mean estimate",
        dictionary="yes",
    ),
]

# --------------------------------------------------------------------------
# PUMD layer
# --------------------------------------------------------------------------

# BLS name -> (english name, type, description, unit, observations)
KEYS = {
    "newid": (
        "newid",
        "STRING",
        "BLS public-use identifier of the consumer unit and interview or diary week; the last digit is the interview number (1-4) or diary week (1-2)",
        "",
        "Unique per interview or diary week",
    ),
    "finlwt21": (
        "final_weight",
        "FLOAT64",
        "Final calibrated weight of the consumer unit for the full sample",
        "",
        "Dimensionless sampling weight",
    ),
    "membno": (
        "member_number",
        "STRING",
        "Sequence number of the member within the consumer unit",
        "",
        "",
    ),
    "ref_yr": (
        "reference_year",
        "INT64",
        "Calendar year the expenditure refers to",
        "year",
        "",
    ),
    "refyr": (
        "reference_year",
        "INT64",
        "Calendar year the income refers to",
        "year",
        "",
    ),
    "ref_mo": (
        "reference_month",
        "INT64",
        "Calendar month the expenditure refers to, from 1 to 12",
        "month",
        "",
    ),
    "refmo": (
        "reference_month",
        "INT64",
        "Calendar month the income refers to, from 1 to 12",
        "month",
        "",
    ),
}
for i in range(1, 45):
    KEYS[f"wtrep{i:02d}"] = (
        f"replicate_weight_{i:02d}",
        "FLOAT64",
        f"Balanced half-sample replicate weight {i} of 44, used to estimate sampling variance",
        "",
        "Dimensionless sampling weight",
    )

ID_LIKE = {
    "ucc",
    "seqno",
    "alcno",
    "uccseq",
    "expname",
    "qredate",
    "strtday",
    "strtmnth",
    "strtyear",
    "qintrvyr",
    "qintrvmo",
    "psu",
    "state",
    "cid",
    "hh_cu_q",
    "hhid",
    "cuid",
    "interi",
}

MONEY = re.compile(
    r"amount|expend|expense|income|cost|value|outlay|\btax|paid|owed|price|"
    r"rent|fee|charge|contribut|payment|receiv|receipt|loss|earn|salary|wage|"
    r"dollar|\$|bracket range|this quarter|last quarter|previous quarter|"
    r"current quarter|purchas|spent|insurance|pension|benefit|annuit|alimony|"
    r"support|deduct|refund|stamps|welfare|interest|dividend|royalt|asset|"
    r"saving|debt|loan|mortgage|equity|worth|gift|cash|bonds|stocks|securit",
    re.IGNORECASE,
)
COUNT = re.compile(
    r"^\s*(number of|#|how many|total number)|\bnumber of (persons|members|"
    r"earners|vehicles|children|rooms|bedrooms|bathrooms|payments|weeks|hours)",
    re.IGNORECASE,
)
# flags whose name BLS truncated so they no longer end in "_"
FLAG_ALIASES = {"pymt_009": "pymt2009"}

ITERATION = re.compile(
    r"^imputation iteration\s*#\s*(\d)\s*-\s*([a-z0-9_]+)", re.IGNORECASE
)


def load_dictionary():
    v = pd.read_excel(DICTIONARY_XLSX, sheet_name="Variables")
    v.columns = [c.strip() for c in v.columns]
    c = pd.read_excel(DICTIONARY_XLSX, sheet_name="Codes ")
    c.columns = [x.strip() for x in c.columns]
    v["var"] = v["Variable Name"].astype(str).str.strip().str.lower()
    v["flag"] = v["Flag name"].astype(str).str.strip().str.lower()
    v["last"] = v["Last year"].fillna(9999)
    v = v.sort_values("last")
    variables = {}  # (FILE, var) -> latest dictionary row
    flags = {}  # (FILE, flag) -> parent var
    for r in v.itertuples(index=False):
        f = str(r.File).strip().upper()
        variables[(f, r.var)] = r
        if r.flag and r.flag not in ("nan", ""):
            flags[(f, r.flag)] = r.var
    coded = {
        (str(f).strip().upper(), str(n).strip().lower())
        for f, n in zip(c["File"], c["Variable"], strict=True)
    }
    return variables, flags, coded


def clean_desc(text: object) -> str:
    text = re.sub(r"\s+", " ", str(text)).strip()
    if text.lower() in ("nan", ""):
        return ""
    text = text[0].upper() + text[1:]
    text = text.rstrip(" .")
    if len(text) > MAX_DESC:
        text = text[: MAX_DESC - 3].rstrip() + "..."
    return text


def coverage(years: list[int], table_years: tuple[int, int]) -> str:
    lo, hi = min(years), max(years)
    if (lo, hi) == table_years:
        return ""
    return f"{lo}(1){hi}"


# Variables the rules below cannot place, checked by hand against the BLS
# dictionary and the data (2026-10-01).
OVERRIDES = {
    "birthyr": ("INT64", "year", "no"),
    "yrbuilt": ("INT64", "year", "no"),
    "povlev": ("FLOAT64", "usd", "no"),
    "povlevpy": ("FLOAT64", "usd", "no"),
    "othastbx": ("FLOAT64", "usd", "no"),
    "chdtxp": ("FLOAT64", "usd", "no"),
    "chdtxph": ("FLOAT64", "usd", "no"),
    "erecvehc": ("FLOAT64", "usd", "no"),
    "erecvehp": ("FLOAT64", "usd", "no"),
    "procfrvg": ("FLOAT64", "usd", "no"),
    "pymt_009": ("FLOAT64", "usd", "no"),
    "inc_rank": ("FLOAT64", "ratio", "no"),
    "inc_rnku": ("FLOAT64", "ratio", "no"),
    "inc_rnkr": ("FLOAT64", "ratio", "no"),
    "inc_rnkm": ("FLOAT64", "ratio", "no"),
    "num_vet": ("FLOAT64", "person", "no"),
    "fsmpfrmx": ("FLOAT64", "usd", "no"),
    # listed in the BLS Codes sheet, but the data holds amounts, counts or ids
    "fsuppx": ("FLOAT64", "usd", "no"),
    "fsuppx1": ("FLOAT64", "usd", "no"),
    "fsuppx2": ("FLOAT64", "usd", "no"),
    "fsuppx3": ("FLOAT64", "usd", "no"),
    "fsuppx4": ("FLOAT64", "usd", "no"),
    "fsuppx5": ("FLOAT64", "usd", "no"),
    "rrx": ("FLOAT64", "usd", "no"),
    "fs_mthi": ("FLOAT64", "month", "no"),
    "weekn": ("STRING", "", "no"),
    "expnyr": ("INT64", "year", "no"),
    "tu_dpndt": ("STRING", "", "no"),
    # coded (1996-2001) then a four-digit year (2011+); kept as text
    "built": ("STRING", "", "no"),
}


def classify(name, desc, file, variables, coded, depth=0, formula=""):
    """Return (type, unit, dictionary) for a non-flag survey variable."""
    if name in OVERRIDES:
        return OVERRIDES[name]
    if (file, name) in coded:
        return "STRING", "", "yes"
    if name in ID_LIKE:
        return "STRING", "", "no"
    m = ITERATION.match(desc)
    if m and depth < 2:
        parent = m.group(2).lower()
        prow = variables.get((file, parent))
        if prow is None:
            # every imputed income iteration is a dollar amount
            return "FLOAT64", "usd", "no"
        klass = classify(
            parent, clean_desc(prow[3]), file, variables, coded, depth + 1
        )
        return klass or ("FLOAT64", "usd", "no")
    if re.search(r"^sum\s*\(", str(formula), re.IGNORECASE):
        return "FLOAT64", "usd", "no"
    if re.search(r"indicator/descriptor", desc, re.IGNORECASE):
        return "STRING", "", "no"
    if re.match(r"^age\b|age of|member's age|^what is the .*age", desc, re.I):
        return "INT64", "year", "no"
    if re.search(r"\bhours?\b", desc, re.I) and COUNT.search(desc):
        return "FLOAT64", "hour", "no"
    if re.search(r"\bweeks?\b", desc, re.I) and COUNT.search(desc):
        return "FLOAT64", "week", "no"
    if COUNT.search(desc):
        return "FLOAT64", "unit", "no"
    if MONEY.search(desc):
        return "FLOAT64", "usd", "no"
    return None


def build_pumd(report: bool):
    variables, flags, coded = load_dictionary()
    files = selected_quarter_files()
    cols = defaultdict(lambda: defaultdict(set))
    order = defaultdict(list)
    for qf in sorted(files, key=lambda q: (q.year, q.quarter), reverse=True):
        for h in read_header(qf):
            if h not in cols[qf.family]:
                order[qf.family].append(h)
            cols[qf.family][h].add(qf.year)

    unresolved = []
    tables = {}
    for family, (survey, slug) in FAMILIES.items():
        file = family.upper()
        years = sorted({y for s in cols[family].values() for y in s})
        span = (years[0], years[-1])
        rows = [
            col(
                "year",
                "INT64",
                f"Year the {'interview' if survey == 'interview' else 'diary'} was collected",
                directory=YEAR_DIR,
                unit="year",
                obs="Partition column. Collection year of the quarter file, not the BLS release year",
                original="file name (YYQ)",
            ),
            col(
                "quarter",
                "INT64",
                "Quarter the data was collected, from 1 to 4",
                unit="quarter",
                original="file name (YYQ)",
            ),
            col(
                "consumer_unit_id",
                "STRING",
                "Identifier of the consumer unit (household), stable across its interviews or diary weeks",
                obs="NEWID without its last digit",
                original="newid",
            ),
            col(
                "interview_number" if survey == "interview" else "diary_week",
                "STRING",
                "Interview number of the consumer unit: 2 to 5 through 2015 (interview 1 was an unreleased bounding interview), 1 to 4 after BLS dropped the bounding interview in 2015"
                if survey == "interview"
                else "Diary week of the consumer unit, 1 or 2",
                obs="Last digit of NEWID",
                original="newid",
            ),
        ]
        body = []
        for h in order[family]:
            yrs = sorted(cols[family][h])
            cov = coverage(yrs, span)
            if h in KEYS:
                en, typ, desc, unit, obs = KEYS[h]
                row = col(
                    en, typ, desc, unit=unit, obs=obs, original=h, coverage=cov
                )
                row["_rank"] = (
                    "0" if h == "newid" else "1" if h == "membno" else "2"
                )
                body.append(row)
                continue
            if (
                h in FLAG_ALIASES
                or (file, h) in flags
                or (
                    h.endswith("_")
                    and ((file, h[:-1]) in variables or h[:-1] in cols[family])
                )
            ):
                parent = FLAG_ALIASES.get(h) or flags.get((file, h), h[:-1])
                row = col(
                    h,
                    "STRING",
                    f"Data-quality flag for {parent.upper()}: valid, blank, allocated, imputed or topcoded",
                    dictionary="yes",
                    obs="Flag codes A-W are listed in dicionario and in the BLS Getting Started Guide",
                    coverage=cov,
                )
                row["_rank"] = "4"
                body.append(row)
                continue
            r = variables.get((file, h))
            desc = clean_desc(r[3]) if r is not None else ""
            formula = r[4] if r is not None else ""
            klass = classify(h, desc, file, variables, coded, formula=formula)
            obs = ""
            if r is None:
                desc = (
                    desc
                    or f"BLS variable {h.upper()}, not documented in the BLS dictionary"
                )
                obs = "Absent from the BLS PUMD dictionary"
            if klass is None:
                klass = ("STRING", "", "no")
                unresolved.append((slug, h, desc))
                obs = (
                    (obs + " " if obs else "")
                    + "Stored as text: type not determinable from the BLS documentation"
                )
            typ, unit, dic = klass
            row = col(
                h,
                typ,
                desc or h.upper(),
                dictionary=dic,
                unit=unit,
                obs=obs,
                coverage=cov,
            )
            row["_rank"] = "3"
            body.append(row)

        # keys, weights, then variables each followed by its flag
        keys = sorted(
            [b for b in body if int(b["_rank"]) <= 2],
            key=lambda b: int(b["_rank"]),
        )
        flag_rows = {b["name"]: b for b in body if b["_rank"] == "4"}
        ordered = list(keys)
        for b in body:
            if b["_rank"] != "3":
                continue
            ordered.append(b)
            f = flag_rows.pop(b["name"] + "_", None)
            if f:
                ordered.append(f)
        ordered.extend(flag_rows.values())
        for b in ordered:
            b.pop("_rank", None)
        tables[slug] = rows + ordered

    if report:
        for slug, h, d in unresolved:
            print(f"UNRESOLVED {slug}.{h}: {d}")
        print(f"{len(unresolved)} unresolved columns")
    return tables


DICIONARIO = [
    col(
        "id_tabela",
        "STRING",
        "Slug of the us_bls_cex table the dictionary entry describes",
    ),
    col(
        "nome_coluna",
        "STRING",
        "Name of the column the dictionary entry describes",
    ),
    col("chave", "STRING", "Coded value (key) exactly as stored in the data"),
    col("cobertura_temporal", "STRING", "Temporal coverage of the key"),
    col(
        "valor",
        "STRING",
        "Human-readable label corresponding to the coded value",
    ),
]

UCC = [
    col(
        "year",
        "INT64",
        "Year of the BLS hierarchical grouping file the row belongs to",
        directory=YEAR_DIR,
        unit="year",
        obs="Partition column",
    ),
    col(
        "hierarchy",
        "STRING",
        "Hierarchical grouping the row belongs to: integrated, interview or diary",
        dictionary="yes",
        obs="From the file name CE-HG-{Integ,Inter,Diary}-YYYY.txt",
    ),
    col(
        "line_number",
        "INT64",
        "Position of the row in the BLS grouping file, preserving the published order",
        unit="unit",
        obs="Ordinal; continuation lines are merged into the row they continue",
    ),
    col(
        "level",
        "INT64",
        "Depth of the row in the category tree, 1 being the top",
        unit="level",
    ),
    col("title", "STRING", "Name of the category, statistic or UCC"),
    col(
        "ucc",
        "STRING",
        "Universal Classification Code, or the BLS mnemonic for title, group and statistic rows",
    ),
    col(
        "row_type",
        "STRING",
        "Row type: H header, T title, G group (sum of its children), S statistic, I interview UCC, D diary UCC",
        dictionary="yes",
        original="type",
    ),
    col(
        "factor",
        "STRING",
        "Aggregation factor BLS applies when summing the row into its group",
        dictionary="yes",
    ),
    col(
        "section",
        "STRING",
        "Section of the grouping: CUCHARS, EXPEND, FOOD, INCOME, ASSETS or ADDENDA",
        dictionary="yes",
    ),
    col(
        "parent_ucc",
        "STRING",
        "UCC or mnemonic of the nearest group or title row one level up",
        obs="Derived from the level column",
    ),
]


def write(slug, rows):
    OUT.mkdir(parents=True, exist_ok=True)
    with open(OUT / f"{slug}.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=HEADER, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
    print(f"{slug}: {len(rows)} columns")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", action="store_true")
    args = ap.parse_args()
    write("series", SERIES)
    write("annual", ANNUAL)
    write("ucc", UCC)
    write("dicionario", DICIONARIO)
    for slug, rows in build_pumd(args.report).items():
        names = [r["name"] for r in rows]
        dup = {n for n in names if names.count(n) > 1}
        if dup:
            raise SystemExit(f"{slug}: duplicate column names {sorted(dup)}")
        write(slug, rows)


if __name__ == "__main__":
    main()
