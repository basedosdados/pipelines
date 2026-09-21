#!/usr/bin/env python3
"""
Build local architecture CSVs for us_ed_ipeds (Data Basis).

For each of the 23 IPEDS tables, reads the parquet schema from
output/<table>/year=YYYY/data.parquet and writes an architecture CSV at
code/architecture/<table>.csv with columns:

    name, bigquery_type, description, temporal_coverage,
    covered_by_dictionary, directory_column, measurement_unit,
    has_sensitive_data, observations, original_name

Descriptions come from the official IPEDS data dictionaries
(https://nces.ed.gov/ipeds/datacenter/data/{FILENAME}_Dict.zip), iterating
years NEWEST first so each column gets its newest available varTitle.
Unresolved columns get description "TODO".

Usage:
    uv run python models/us_ed_ipeds/code/build_architecture.py
"""

import html as htmllib
import io
import logging
import re
import sys
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
# pyrefly: ignore [missing-import]
from clean_data import build_manifest

BASE_URL = "https://nces.ed.gov/ipeds/datacenter/data/"
ROOT = Path(__file__).resolve().parents[1]
INPUT_DIR = ROOT / "input"
OUTPUT_DIR = ROOT / "output"
ARCH_DIR = ROOT / "code" / "architecture"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("arch")

ARCH_COLUMNS = [
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

YEAR_DESCRIPTION = (
    "Year of the survey data collection "
    "(fall of academic year for most components)"
)

# ---------------------------------------------------------------------------
# Reverse rename maps (cleaned name -> candidate original IPEDS varnames,
# newest-first). Derived from harmonize_hd, harmonize_adm, harmonize_ic_ay,
# and harmonize_completions in clean_data.py.
# ---------------------------------------------------------------------------

HD_REV = {
    "institution_name": ["instnm"],
    "state": ["stabbr"],
    "zip_code": ["zip"],
    "fips_state": ["fips"],
    "level": ["iclevel"],
    "carnegie_basic": [
        "c21basic",
        "c18basic",
        "c15basic",
        "ccbasic",
        "carnegie",
    ],
    "degree_granting": ["deggrant"],
    "land_grant": ["landgrnt"],
    "locale_code": ["locale"],
    "longitude": ["longitud"],
    "size_category": ["instsize"],
    "currently_active": ["cyactive"],
    "close_date": ["closedat"],
    "region": ["obereg"],
    "county_name": ["countynm"],
    "county_fips": ["countycd"],
    "website": ["webaddr"],
}

ADM_REV = {
    "applicants_total": ["applcn"],
    "applicants_men": ["applcnm"],
    "applicants_women": ["applcnw"],
    "admissions_total": ["admssn"],
    "admissions_men": ["admssnm"],
    "admissions_women": ["admssnw"],
    "enrolled_total": ["enrlt"],
    "enrolled_men": ["enrlm"],
    "enrolled_women": ["enrlw"],
    "sat_verbal_25th": ["satvr25"],
    "sat_verbal_75th": ["satvr75"],
    "sat_math_25th": ["satmt25"],
    "sat_math_75th": ["satmt75"],
    "sat_writing_25th": ["satwr25"],
    "sat_writing_75th": ["satwr75"],
    "act_composite_25th": ["actcm25"],
    "act_composite_75th": ["actcm75"],
    "act_english_25th": ["acten25"],
    "act_english_75th": ["acten75"],
    "act_math_25th": ["actmt25"],
    "act_math_75th": ["actmt75"],
    "act_writing_25th": ["actwr25"],
    "act_writing_75th": ["actwr75"],
    "sat_ebrw_25th": ["saterdw25"],
    "sat_ebrw_75th": ["saterdw75"],
}

IC_AY_REV = {
    "tuition_in_state": ["tuition2", "chg2ay2"],
    "tuition_out_state": ["tuition3", "chg2ay3"],
    "tuition_in_district": ["tuition1", "chg2ay1"],
}

C_A_REV = {
    "award_level": ["awlevel"],
    "major_number": ["majornum"],
}

REVERSE_MAPS = {
    "hd": HD_REV,
    "adm": ADM_REV,
    "ic_ay": IC_AY_REV,
    "c_a": C_A_REV,
}

MONEY_KEYWORDS = ["dollar", "amount charged", "salar"]

# ---------------------------------------------------------------------------
# Dictionary download and parsing
# ---------------------------------------------------------------------------

_failed_downloads: set[str] = set()
_unparsed: list[str] = []


def download_dict(filename: str) -> Path | None:
    """Download {filename}_Dict.zip into input/ (cached)."""
    local = INPUT_DIR / f"{filename}_Dict.zip"
    if local.exists() and local.stat().st_size > 0:
        return local
    if filename in _failed_downloads:
        return None
    url = f"{BASE_URL}{filename}_Dict.zip"
    try:
        log.info(f"  Downloading {url}")
        resp = requests.get(url, timeout=60)
    except requests.exceptions.RequestException as e:
        log.warning(f"  Download error for {filename}_Dict: {e}")
        _failed_downloads.add(filename)
        return None
    if resp.status_code != 200:
        log.warning(f"  HTTP {resp.status_code} for {filename}_Dict.zip")
        _failed_downloads.add(filename)
        return None
    local.write_bytes(resp.content)
    return local


def _parse_excel_varlist(raw: bytes, engine: str) -> dict[str, str]:
    """Parse a varlist sheet from xlsx/xls bytes. Returns varname->title."""
    entries: dict[str, str] = {}
    # pyrefly: ignore [bad-argument-type]
    xl = pd.ExcelFile(io.BytesIO(raw), engine=engine)
    sheet = next(
        # pyrefly: ignore [missing-attribute]
        (s for s in xl.sheet_names if s.strip().lower() == "varlist"),
        None,
    )
    if sheet is None:
        return entries
    df = xl.parse(sheet)
    # pyrefly: ignore [unnecessary-type-conversion]
    cols = {str(c).strip().lower(): c for c in df.columns}
    name_col = cols.get("varname")
    title_col = cols.get("vartitle")
    if name_col is None or title_col is None:
        return entries
    imp_col = cols.get("imputationvar")
    for _, row in df.iterrows():
        vn = row[name_col]
        vt = row[title_col]
        if pd.isna(vn) or pd.isna(vt):
            continue
        vn = str(vn).strip().lower()
        vt = str(vt).strip()
        if vn and vt and vn not in entries:
            entries[vn] = vt
        # Imputation flag variables often lack their own row; synthesize one.
        if imp_col is not None and pd.notna(row[imp_col]):
            iv = str(row[imp_col]).strip().lower()
            if iv and iv not in entries:
                entries[iv] = f"Imputation field for {vn.upper()} - {vt}"
    return entries


_HTML_SUMMARY_RE = re.compile(
    r"([A-Za-z][A-Za-z0-9_]{1,29})-([^<>\r\n]{2,200}?)<hr>", re.IGNORECASE
)
_HTML_DETAIL_RE = re.compile(
    r"([A-Za-z][A-Za-z0-9_]{1,29})-\d+-([^<>\r\n]{2,200})", re.IGNORECASE
)


def _parse_html_dict(raw: bytes) -> dict[str, str]:
    """Parse an old-style HTML data dictionary. Returns varname->title."""
    text = raw.decode("latin-1", errors="replace")
    entries: dict[str, str] = {}
    for regex in (_HTML_SUMMARY_RE, _HTML_DETAIL_RE):
        for m in regex.finditer(text):
            vn = m.group(1).strip().lower()
            vt = htmllib.unescape(m.group(2)).strip()
            if vn and vt and vn not in entries:
                entries[vn] = vt
    return entries


def parse_dict_zip(zip_path: Path) -> dict[str, str]:
    """Parse a dictionary zip into varname (lowercase) -> varTitle."""
    entries: dict[str, str] = {}
    try:
        with zipfile.ZipFile(zip_path) as zf:
            for member in zf.namelist():
                low = member.lower()
                try:
                    raw = zf.read(member)
                    if low.endswith(".xlsx"):
                        parsed = _parse_excel_varlist(raw, "openpyxl")
                    elif low.endswith(".xls"):
                        parsed = _parse_excel_varlist(raw, "xlrd")
                    elif low.endswith(".html") or low.endswith(".htm"):
                        parsed = _parse_html_dict(raw)
                    else:
                        continue
                    for k, v in parsed.items():
                        entries.setdefault(k, v)
                except Exception as e:
                    log.warning(
                        f"  Could not parse {member} in {zip_path.name}: {e}"
                    )
                    _unparsed.append(f"{zip_path.name}/{member}: {e}")
    except zipfile.BadZipFile as e:
        log.warning(f"  Bad zip {zip_path.name}: {e}")
        _unparsed.append(f"{zip_path.name}: {e}")
    return entries


# ---------------------------------------------------------------------------
# Per-table architecture builder
# ---------------------------------------------------------------------------


def read_table_columns(table: str) -> list[tuple[str, str]]:
    """Return [(colname, bigquery_type)] from the newest parquet partition."""
    parts = sorted((OUTPUT_DIR / table).glob("year=*/data.parquet"))
    if not parts:
        raise FileNotFoundError(f"No parquet found for {table}")
    schema = pq.read_schema(parts[-1])
    cols = []
    for field in schema:
        t = field.type
        if str(t).startswith("int"):
            bq = "INT64"
        elif str(t) in ("double", "float", "float64", "float32"):
            bq = "FLOAT64"
        elif str(t) == "bool":
            bq = "BOOL"
        else:
            bq = "STRING"
        cols.append((field.name, bq))
    # Order: unitid first among data columns (year handled separately)
    cols.sort(key=lambda c: 0 if c[0] == "unitid" else 1)
    # sort is stable, so remaining columns keep schema order
    return cols


def build_table(table: str, file_list: list[tuple[int, str]]) -> dict:
    log.info(f"=== {table} ===")
    cols = read_table_columns(table)
    rev = REVERSE_MAPS.get(table, {})

    # Candidate original varnames per column (newest-first)
    candidates = {name: rev.get(name, [name]) for name, _ in cols}

    resolved: dict[
        str, tuple[str, str]
    ] = {}  # col -> (description, matched_varname)
    for _year, filename in file_list:  # manifest is already newest-first
        unresolved = [c for c in candidates if c not in resolved]
        if not unresolved:
            break
        zip_path = download_dict(filename)
        if zip_path is None:
            continue
        entries = parse_dict_zip(zip_path)
        if not entries:
            continue
        for col in unresolved:
            for cand in candidates[col]:
                if cand in entries:
                    resolved[col] = (entries[cand], cand)
                    break

    rows = []
    # year row first
    rows.append(
        {
            "name": "year",
            "bigquery_type": "INT64",
            "description": YEAR_DESCRIPTION,
            "temporal_coverage": "",
            "covered_by_dictionary": "no",
            "directory_column": "br_bd_diretorios_data_tempo.ano:ano",
            "measurement_unit": "",
            "has_sensitive_data": "no",
            "observations": "",
            "original_name": "",
        }
    )
    n_desc = 0
    for name, bq in cols:
        desc, matched = resolved.get(name, ("TODO", None))
        if desc != "TODO":
            n_desc += 1
        original = matched if matched else candidates[name][0]
        unit = ""
        if desc != "TODO" and any(k in desc.lower() for k in MONEY_KEYWORDS):
            unit = "USD"
        rows.append(
            {
                "name": name,
                "bigquery_type": bq,
                "description": desc,
                "temporal_coverage": "",
                "covered_by_dictionary": "no",
                "directory_column": "",
                "measurement_unit": unit,
                "has_sensitive_data": "no",
                "observations": "",
                "original_name": original,
            }
        )

    df = pd.DataFrame(rows, columns=ARCH_COLUMNS)
    ARCH_DIR.mkdir(parents=True, exist_ok=True)
    out_path = ARCH_DIR / f"{table}.csv"
    df.to_csv(out_path, index=False)
    total = len(cols)
    log.info(
        f"  {out_path.name}: {total} data cols, {n_desc} described, {total - n_desc} TODO"
    )
    return {"columns": total, "described": n_desc, "todo": total - n_desc}


def main():
    manifest = build_manifest()
    tables = sorted(manifest.keys())
    if len(sys.argv) > 1:
        tables = [t for t in tables if t in set(sys.argv[1:])]

    report: dict[str, dict] = {}
    for table in tables:
        report[table] = build_table(table, manifest[table])

    lines = ["# Architecture coverage report — us_ed_ipeds", ""]
    lines.append("| table | columns | described | TODO |")
    lines.append("|-------|---------|-----------|------|")
    tot_c = tot_d = tot_t = 0
    for table in tables:
        r = report[table]
        tot_c += r["columns"]
        tot_d += r["described"]
        tot_t += r["todo"]
        lines.append(
            f"| {table} | {r['columns']} | {r['described']} | {r['todo']} |"
        )
    lines.append(f"| **total** | {tot_c} | {tot_d} | {tot_t} |")
    lines.append("")
    if _failed_downloads:
        lines.append("## Dictionaries not downloadable")
        lines.append("")
        for f in sorted(_failed_downloads):
            lines.append(f"- {f}_Dict.zip")
        lines.append("")
    if _unparsed:
        lines.append("## Dictionary files that could not be parsed")
        lines.append("")
        for f in _unparsed:
            lines.append(f"- {f}")
        lines.append("")

    (ARCH_DIR / "COVERAGE.md").write_text("\n".join(lines))
    print("\n".join(lines))


if __name__ == "__main__":
    main()
