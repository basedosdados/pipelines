#!/usr/bin/env python3
"""
IPEDS cleaning pipeline for Data Basis (us_ed_ipeds)
====================================================
Downloads, parses, cleans, and harmonizes IPEDS data from the National Center
for Education Statistics (NCES) into partitioned Parquet files ready for
upload to BigQuery.

Adapted from https://github.com/paulgp/ipeds-database (MIT License,
Paul Goldsmith-Pinkham). The download, parsing, and harmonization logic is
kept verbatim; the DuckDB loading step is replaced by partitioned Parquet
output (output/<table>/year=YYYY/data.parquet, snappy compression).

Usage:
    uv run python models/us_ed_ipeds/code/clean_data.py            # all tables
    uv run python models/us_ed_ipeds/code/clean_data.py adm ef_d   # subset

Raw ZIP files are cached in models/us_ed_ipeds/input/ so subsequent runs
skip downloads.

Survey components (23 tables):
    hd       Institutional directory (name, location, control, Carnegie)
    ic       Institutional characteristics (programs, services, athletics)
    ic_ay    Academic year tuition and fees
    ic_py    Program year tuition and fees
    adm      Admissions (applicants, admits, enrolled, SAT/ACT)
    efia     12-month instructional activity (FTE)
    effy     12-month unduplicated headcount enrollment
    ef_a     Fall enrollment by race/ethnicity and gender
    ef_b     Fall enrollment by age
    ef_c     Residence and migration of first-time students
    ef_d     Retention rates
    c_a      Completions: degrees/awards by CIP code, race, gender
    sfa      Student financial aid (grants, loans, net price)
    gr       Graduation rates (150% of normal time)
    gr200    Graduation rates (200% of normal time)
    om       Outcome measures (8-year outcomes)
    eap      Employees by assigned position
    sal_is   Salaries of instructional staff
    al       Academic libraries
    f1a      Finance: GASB (public institutions) — revenue, expenses, endowment
    f2       Finance: FASB (private nonprofit) — revenue, expenses, endowment
    f3       Finance: for-profit institutions — revenue, expenses
    flags    Survey response status flags
"""

import io
import logging
import re
import sys
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = "https://nces.ed.gov/ipeds/datacenter/data/"
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "input"
OUTPUT_DIR = ROOT / "output"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("ipeds")

# ---------------------------------------------------------------------------
# File manifest: survey -> list of (year, filename)
# We define these explicitly so we have full control over what goes in.
# ---------------------------------------------------------------------------


def build_manifest():
    """Build the file manifest from the discovered files."""
    manifest = {}

    # HD: Directory information
    manifest["hd"] = [(y, f"HD{y}") for y in range(2024, 2001, -1)]

    # IC: Institutional characteristics
    manifest["ic"] = [(y, f"IC{y}") for y in range(2024, 1999, -1)]

    # IC_AY: Academic year prices
    manifest["ic_ay"] = [(y, f"IC{y}_AY") for y in range(2023, 1999, -1)]

    # IC_PY: Program year prices
    manifest["ic_py"] = [(y, f"IC{y}_PY") for y in range(2023, 1999, -1)]

    # ADM: Admissions
    manifest["adm"] = [(y, f"ADM{y}") for y in range(2023, 2013, -1)]

    # EFIA: 12-month instructional activity
    manifest["efia"] = [(y, f"EFIA{y}") for y in range(2024, 2001, -1)]

    # EFFY: 12-month unduplicated headcount
    manifest["effy"] = [(y, f"EFFY{y}") for y in range(2024, 2001, -1)]

    # EFA: Fall enrollment by race/ethnicity (A component)
    manifest["ef_a"] = [(y, f"EF{y}A") for y in range(2023, 1999, -1)]

    # EFB: Fall enrollment by age
    manifest["ef_b"] = [(y, f"EF{y}B") for y in range(2023, 1999, -1)]

    # EFC: Residence and migration
    manifest["ef_c"] = [(y, f"EF{y}C") for y in range(2023, 1999, -1)]

    # EFD: Retention rates
    manifest["ef_d"] = [(y, f"EF{y}D") for y in range(2023, 1999, -1)]

    # C_A: Completions awards
    manifest["c_a"] = [(y, f"C{y}_A") for y in range(2024, 1999, -1)]

    # SFA: Student financial aid (uses academic year naming)
    sfa_files = []
    sfa_files.append((2023, "SFA2223"))
    sfa_files.append((2022, "SFA2122"))
    sfa_files.append((2021, "SFA2021"))
    sfa_files.append((2020, "SFA1920"))
    sfa_files.append((2019, "SFA1819"))
    sfa_files.append((2018, "SFA1718"))
    sfa_files.append((2017, "SFA1617"))
    sfa_files.append((2016, "SFA1516"))
    sfa_files.append((2015, "SFA1415"))
    sfa_files.append((2014, "SFA1314"))
    sfa_files.append((2013, "SFA1213"))
    sfa_files.append((2012, "SFA1112"))
    sfa_files.append((2011, "SFA1011"))
    sfa_files.append((2010, "SFA0910"))
    sfa_files.append((2009, "SFA0809"))
    sfa_files.append((2008, "SFA0708"))
    sfa_files.append((2007, "SFA0607"))
    sfa_files.append((2006, "SFA0506"))
    sfa_files.append((2005, "SFA0405"))
    sfa_files.append((2004, "SFA0304"))
    sfa_files.append((2003, "SFA0203"))
    sfa_files.append((2002, "SFA0102"))
    manifest["sfa"] = sfa_files

    # GR: Graduation rates 150%
    manifest["gr"] = [(y, f"GR{y}") for y in range(2023, 1996, -1)]

    # GR200: Graduation rates 200% (uses 2-digit year in filename)
    manifest["gr200"] = [
        (2000 + y, f"GR200_{y:02d}") for y in range(23, 7, -1)
    ]

    # OM: Outcome measures
    manifest["om"] = [(y, f"OM{y}") for y in range(2023, 2014, -1)]

    # EAP: Employees by assigned position
    manifest["eap"] = [(y, f"EAP{y}") for y in range(2023, 2000, -1)]

    # SAL_IS: Salaries - instructional staff
    manifest["sal_is"] = [(y, f"SAL{y}_IS") for y in range(2023, 2011, -1)]

    # SAL: Salaries (older format, pre-2012) - these files use different naming;
    # the SAL{year}_IS format only starts in 2012, and pre-2012 data uses
    # different file structures. Omitting for now as the naming convention changed.
    # manifest["sal"] = [(y, f"SAL{y}") for y in range(2011, 2000, -1)]

    # AL: Academic libraries
    manifest["al"] = [(y, f"AL{y}") for y in range(2023, 2013, -1)]

    # FLAGS: Response status
    manifest["flags"] = [(y, f"FLAGS{y}") for y in range(2024, 2003, -1)]

    # Finance: F1A (GASB/public), F2 (FASB/private nonprofit), F3 (for-profit)
    # Uses academic year naming like SFA (e.g., F2223_F1A for fiscal year ending 2023)
    def _fin_files(suffix, start_yr=2001):
        files = []
        for end_yr in range(2023, start_yr, -1):
            yr_code = f"{(end_yr - 1) % 100:02d}{end_yr % 100:02d}"
            files.append((end_yr, f"F{yr_code}_{suffix}"))
        return files

    manifest["f1a"] = _fin_files("F1A", start_yr=2001)  # 2002-2023
    manifest["f2"] = _fin_files("F2", start_yr=2000)  # 2001-2023
    manifest["f3"] = _fin_files("F3", start_yr=2000)  # 2001-2023

    return manifest


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------


def download_file(filename: str, retries: int = 3) -> Path:
    """Download a zip file if not already cached."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    local_path = DATA_DIR / f"{filename}.zip"
    if local_path.exists() and local_path.stat().st_size > 0:
        return local_path

    url = f"{BASE_URL}{filename}.zip"
    for attempt in range(retries):
        try:
            log.info(
                f"Downloading {url}"
                + (f" (attempt {attempt + 1})" if attempt > 0 else "")
            )
            resp = requests.get(url, timeout=120, stream=False)
            if resp.status_code != 200:
                log.warning(
                    f"  Failed to download {filename}: HTTP {resp.status_code}"
                )
                return None
            local_path.write_bytes(resp.content)
            return local_path
        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
        ) as e:
            log.warning(
                f"  Download error (attempt {attempt + 1}/{retries}): {e}"
            )
            if attempt == retries - 1:
                return None
            import time

            time.sleep(2**attempt)
    return None


# ---------------------------------------------------------------------------
# CSV Reading
# ---------------------------------------------------------------------------


def read_csv_from_zip(zip_path: Path) -> pd.DataFrame:
    """Extract and read the CSV data file from a zip archive."""
    with zipfile.ZipFile(zip_path) as zf:
        # Find the main CSV file (not the dictionary/frequencies file)
        csv_files = [
            f
            for f in zf.namelist()
            if f.lower().endswith(".csv")
            and "_dict" not in f.lower()
            and "_freq" not in f.lower()
            and "_rv" not in f.lower()
        ]
        if not csv_files:
            # Maybe the data file has a different extension
            csv_files = [
                f for f in zf.namelist() if f.lower().endswith(".csv")
            ]

        if not csv_files:
            log.warning(f"  No CSV found in {zip_path.name}: {zf.namelist()}")
            return None

        # Pick the largest CSV (usually the data file)
        csv_file = max(csv_files, key=lambda f: zf.getinfo(f).file_size)

        with zf.open(csv_file) as f:
            raw = f.read()

        # Try common encodings
        for encoding in ["utf-8", "latin-1", "cp1252"]:
            try:
                text = raw.decode(encoding)
                break
            except UnicodeDecodeError:
                continue
        else:
            text = raw.decode("latin-1", errors="replace")

        # Parse CSV
        try:
            # index_col=False: IC*_PY files 2004+ have one extra trailing
            # field per data line; without it pandas promotes UNITID to the
            # index and shifts every column left by one
            df = pd.read_csv(
                io.StringIO(text),
                low_memory=False,
                on_bad_lines="skip",
                encoding_errors="replace",
                index_col=False,
            )
        except Exception as e:
            log.warning(f"  Error reading CSV from {zip_path.name}: {e}")
            return None

        # Drop pandas duplicate-header artifacts (e.g. "xefgndru.1" in
        # EF2022A) and the stray all-null "i" column in some finance files
        junk = [
            c
            for c in df.columns
            if re.match(r".+\.\d+$", str(c))
            or (str(c).strip().lower() == "i" and df[c].isna().all())
        ]
        if junk:
            df = df.drop(columns=junk)
            log.info(f"  Dropped junk columns from {zip_path.name}: {junk}")

        return df


# ---------------------------------------------------------------------------
# Cleaning utilities
# ---------------------------------------------------------------------------


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names to lowercase, strip whitespace."""
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def coerce_unitid(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure UNITID is integer type."""
    if "unitid" in df.columns:
        df["unitid"] = pd.to_numeric(df["unitid"], errors="coerce")
        df = df.dropna(subset=["unitid"])
        df["unitid"] = df["unitid"].astype(int)
    return df


def clean_numeric_columns(df: pd.DataFrame, exclude_cols=None) -> pd.DataFrame:
    """
    Convert columns that should be numeric but contain IPEDS special values.
    IPEDS uses various codes for missing/not applicable:
      - Empty/blank
      - '.' (SAS missing)
      - 'A' through 'Z' (various imputation flags when embedded)
    """
    exclude = set(exclude_cols or [])
    exclude.update(
        [
            "unitid",
            "instnm",
            "addr",
            "city",
            "stabbr",
            "zip",
            "chfnm",
            "chftitle",
            "gentele",
            "ein",
            "webaddr",
            "adminurl",
            "faidurl",
            "applurl",
            "npricurl",
            "opeid",
            "countynm",
            "countycd",
            "longitud",
            "latitude",
            "closedat",
            "cyactive",
            "ialias",
        ]
    )

    for col in df.columns:
        if col in exclude:
            continue
        if df[col].dtype == object:
            # Check if this looks numeric
            sample = df[col].dropna().head(100)
            if len(sample) == 0:
                continue
            # Try converting - IPEDS uses '.' for missing
            converted = pd.to_numeric(
                df[col].replace({".": None, "": None}), errors="coerce"
            )
            # If we can convert most values, do it
            non_null_orig = df[col].notna().sum()
            non_null_conv = converted.notna().sum()
            if non_null_orig > 0 and non_null_conv / non_null_orig > 0.5:
                df[col] = converted

    return df


# ---------------------------------------------------------------------------
# Survey-specific harmonization
# ---------------------------------------------------------------------------


def harmonize_hd(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Harmonize institutional directory (HD) data."""
    df = clean_column_names(df)
    df = coerce_unitid(df)
    df["year"] = year

    # Key columns to keep (harmonize names across years)
    # Column renames for consistency
    renames = {}

    # Institution name
    for old in ["instnm"]:
        if old in df.columns:
            renames[old] = "institution_name"

    # State
    for old in ["stabbr"]:
        if old in df.columns:
            renames[old] = "state"

    # City
    if "city" in df.columns:
        renames["city"] = "city"

    # ZIP
    if "zip" in df.columns:
        renames["zip"] = "zip_code"

    # FIPS state code
    if "fips" in df.columns:
        renames["fips"] = "fips_state"

    # Sector (public/private/for-profit, 4yr/2yr/less)
    if "sector" in df.columns:
        renames["sector"] = "sector"

    # Control of institution
    if "control" in df.columns:
        renames["control"] = "control"

    # Level (4-year, 2-year, less-than-2-year)
    if "iclevel" in df.columns:
        renames["iclevel"] = "level"

    # Carnegie classification
    for old in ["c21basic", "c18basic", "c15basic", "ccbasic", "carnegie"]:
        if old in df.columns:
            renames[old] = "carnegie_basic"
            break

    # Degree-granting status
    if "deggrant" in df.columns:
        renames["deggrant"] = "degree_granting"

    # HBCU
    if "hbcu" in df.columns:
        renames["hbcu"] = "hbcu"

    # Hospital
    if "hospital" in df.columns:
        renames["hospital"] = "hospital"

    # Medical
    if "medical" in df.columns:
        renames["medical"] = "medical"

    # Land grant
    if "landgrnt" in df.columns:
        renames["landgrnt"] = "land_grant"

    # Tribal
    if "tribal" in df.columns:
        renames["tribal"] = "tribal"

    # Locale
    if "locale" in df.columns:
        renames["locale"] = "locale_code"

    # OPEID
    if "opeid" in df.columns:
        renames["opeid"] = "opeid"

    # Geographic coordinates
    if "longitud" in df.columns:
        renames["longitud"] = "longitude"
    if "latitude" in df.columns:
        renames["latitude"] = "latitude"

    # Size category
    if "instsize" in df.columns:
        renames["instsize"] = "size_category"

    # Active status
    if "cyactive" in df.columns:
        renames["cyactive"] = "currently_active"

    # Close date
    if "closedat" in df.columns:
        renames["closedat"] = "close_date"

    # Region
    if "obereg" in df.columns:
        renames["obereg"] = "region"

    # County
    if "countynm" in df.columns:
        renames["countynm"] = "county_name"
    if "countycd" in df.columns:
        renames["countycd"] = "county_fips"

    # Website
    if "webaddr" in df.columns:
        renames["webaddr"] = "website"

    df = df.rename(columns=renames)

    # Keep all columns (renamed + original unrenamed)
    return df


def harmonize_ic(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Harmonize institutional characteristics."""
    df = clean_column_names(df)
    df = coerce_unitid(df)
    df["year"] = year
    df = clean_numeric_columns(df)
    return df


def harmonize_ic_ay(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Harmonize academic year prices."""
    df = clean_column_names(df)
    df = coerce_unitid(df)
    df["year"] = year

    renames = {}
    # Tuition and fees
    for old in ["tuition2", "chg2ay2"]:  # in-state tuition
        if old in df.columns:
            renames[old] = "tuition_in_state"
            break
    for old in ["tuition3", "chg2ay3"]:  # out-of-state tuition
        if old in df.columns:
            renames[old] = "tuition_out_state"
            break
    for old in ["tuition1", "chg2ay1"]:  # in-district tuition
        if old in df.columns:
            renames[old] = "tuition_in_district"
            break

    df = df.rename(columns=renames)
    df = clean_numeric_columns(df)
    return df


def harmonize_ic_py(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Harmonize program year prices."""
    df = clean_column_names(df)
    df = coerce_unitid(df)
    df["year"] = year
    df = clean_numeric_columns(df)
    return df


def harmonize_adm(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Harmonize admissions data."""
    df = clean_column_names(df)
    df = coerce_unitid(df)
    df["year"] = year

    renames = {}
    # Applicants
    if "applcn" in df.columns:
        renames["applcn"] = "applicants_total"
    if "applcnm" in df.columns:
        renames["applcnm"] = "applicants_men"
    if "applcnw" in df.columns:
        renames["applcnw"] = "applicants_women"

    # Admissions
    if "admssn" in df.columns:
        renames["admssn"] = "admissions_total"
    if "admssnm" in df.columns:
        renames["admssnm"] = "admissions_men"
    if "admssnw" in df.columns:
        renames["admssnw"] = "admissions_women"

    # Enrolled
    if "enrlt" in df.columns:
        renames["enrlt"] = "enrolled_total"
    if "enrlm" in df.columns:
        renames["enrlm"] = "enrolled_men"
    if "enrlw" in df.columns:
        renames["enrlw"] = "enrolled_women"

    # Test scores
    for prefix, new_prefix in [
        ("satvr", "sat_verbal"),
        ("satmt", "sat_math"),
        ("satwr", "sat_writing"),
        ("actcm", "act_composite"),
        ("acten", "act_english"),
        ("actmt", "act_math"),
        ("actwr", "act_writing"),
    ]:
        if f"{prefix}25" in df.columns:
            renames[f"{prefix}25"] = f"{new_prefix}_25th"
        if f"{prefix}75" in df.columns:
            renames[f"{prefix}75"] = f"{new_prefix}_75th"

    # SAT Evidence-Based Reading and Writing (newer)
    if "satvr25" not in df.columns:
        for old, new in [
            ("saterdw25", "sat_ebrw_25th"),
            ("saterdw75", "sat_ebrw_75th"),
        ]:
            if old in df.columns:
                renames[old] = new

    df = df.rename(columns=renames)
    df = clean_numeric_columns(df)
    return df


def harmonize_efia(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Harmonize 12-month instructional activity."""
    df = clean_column_names(df)
    df = coerce_unitid(df)
    df["year"] = year
    df = clean_numeric_columns(df)
    return df


def harmonize_effy(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Harmonize 12-month unduplicated headcount."""
    df = clean_column_names(df)
    df = coerce_unitid(df)
    df["year"] = year
    df = clean_numeric_columns(df)
    return df


def harmonize_ef_enrollment(
    df: pd.DataFrame, year: int, component: str
) -> pd.DataFrame:
    """Harmonize fall enrollment data (A, B, C, D components)."""
    df = clean_column_names(df)
    df = coerce_unitid(df)
    df["year"] = year
    df = clean_numeric_columns(df)
    return df


def harmonize_completions(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Harmonize completions awards data."""
    df = clean_column_names(df)
    df = coerce_unitid(df)
    df["year"] = year

    renames = {}
    # Award level
    if "awlevel" in df.columns:
        renames["awlevel"] = "award_level"
    if "majornum" in df.columns:
        renames["majornum"] = "major_number"

    df = df.rename(columns=renames)
    df = clean_numeric_columns(df)
    return df


def harmonize_sfa(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Harmonize student financial aid data."""
    df = clean_column_names(df)
    df = coerce_unitid(df)
    df["year"] = year
    df = clean_numeric_columns(df)
    return df


def harmonize_gr(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Harmonize graduation rate data."""
    df = clean_column_names(df)
    df = coerce_unitid(df)
    df["year"] = year
    df = clean_numeric_columns(df)
    return df


def harmonize_gr200(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Harmonize 200% graduation rate data."""
    df = clean_column_names(df)
    df = coerce_unitid(df)
    df["year"] = year
    df = clean_numeric_columns(df)
    return df


def harmonize_om(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Harmonize outcome measures data."""
    df = clean_column_names(df)
    df = coerce_unitid(df)
    df["year"] = year
    df = clean_numeric_columns(df)
    return df


def harmonize_eap(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Harmonize employees by assigned position."""
    df = clean_column_names(df)
    df = coerce_unitid(df)
    df["year"] = year
    df = clean_numeric_columns(df)
    return df


def harmonize_sal(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Harmonize salary data."""
    df = clean_column_names(df)
    df = coerce_unitid(df)
    df["year"] = year
    df = clean_numeric_columns(df)
    return df


def harmonize_al(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Harmonize academic library data."""
    df = clean_column_names(df)
    df = coerce_unitid(df)
    df["year"] = year
    df = clean_numeric_columns(df)
    return df


def harmonize_finance(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Harmonize finance data (F1A, F2, F3)."""
    df = clean_column_names(df)
    df = coerce_unitid(df)
    df["year"] = year
    df = clean_numeric_columns(df)
    return df


def harmonize_flags(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Harmonize response flags."""
    df = clean_column_names(df)
    df = coerce_unitid(df)
    df["year"] = year
    return df


def harmonize_generic(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Generic harmonization for any survey."""
    df = clean_column_names(df)
    df = coerce_unitid(df)
    df["year"] = year
    df = clean_numeric_columns(df)
    return df


# Map survey -> harmonization function
HARMONIZERS = {
    "hd": harmonize_hd,
    "ic": harmonize_ic,
    "ic_ay": harmonize_ic_ay,
    "ic_py": harmonize_ic_py,
    "adm": harmonize_adm,
    "efia": harmonize_efia,
    "effy": harmonize_effy,
    "ef_a": lambda df, y: harmonize_ef_enrollment(df, y, "A"),
    "ef_b": lambda df, y: harmonize_ef_enrollment(df, y, "B"),
    "ef_c": lambda df, y: harmonize_ef_enrollment(df, y, "C"),
    "ef_d": lambda df, y: harmonize_ef_enrollment(df, y, "D"),
    "c_a": harmonize_completions,
    "sfa": harmonize_sfa,
    "gr": harmonize_gr,
    "gr200": harmonize_gr200,
    "om": harmonize_om,
    "eap": harmonize_eap,
    "sal_is": harmonize_sal,
    "sal": harmonize_sal,
    "al": harmonize_al,
    "f1a": harmonize_finance,
    "f2": harmonize_finance,
    "f3": harmonize_finance,
    "flags": harmonize_flags,
}


# ---------------------------------------------------------------------------
# Schema harmonization across years
# ---------------------------------------------------------------------------


def unify_schemas(frames: list[pd.DataFrame]) -> list[pd.DataFrame]:
    """
    Given a list of DataFrames for the same survey across years,
    unify their schemas by filling missing columns with NaN.
    """
    if not frames:
        return frames

    # Collect all columns
    all_cols = []
    seen = set()
    for df in frames:
        for c in df.columns:
            if c not in seen:
                all_cols.append(c)
                seen.add(c)

    # Reindex each frame
    unified = []
    for df in frames:
        for col in all_cols:
            if col not in df.columns:
                df[col] = None
        unified.append(df[all_cols])

    return unified


# ---------------------------------------------------------------------------
# Parquet output
# ---------------------------------------------------------------------------


def build_arrow_schema(
    df: pd.DataFrame, partition_col: str = "year"
) -> pa.Schema:
    """
    Build an explicit pyarrow schema from the combined DataFrame so all
    year partitions share identical types (prevents INT64/FLOAT64 mismatches
    in BigQuery external staging tables).
    """
    fields = []
    for col in df.columns:
        if col == partition_col:
            continue
        dtype = df[col].dtype
        if pd.api.types.is_integer_dtype(dtype):
            typ = pa.int64()
        elif pd.api.types.is_float_dtype(dtype):
            typ = pa.float64()
        elif pd.api.types.is_bool_dtype(dtype):
            typ = pa.bool_()
        else:
            typ = pa.string()
        fields.append(pa.field(col, typ))
    return pa.schema(fields)


def write_survey_parquet(survey: str, combined: pd.DataFrame) -> int:
    """
    Write the combined survey DataFrame to hive-partitioned Parquet:
        output/<survey>/year=YYYY/data.parquet
    Returns total rows written.
    """
    # Object columns -> string (preserve NaN as null) so the explicit schema
    # cast never fails on mixed-type columns
    for col in combined.columns:
        if combined[col].dtype == object:
            mask = combined[col].notna()
            combined.loc[mask, col] = combined.loc[mask, col].astype(str)

    schema = build_arrow_schema(combined)

    table_dir = OUTPUT_DIR / survey
    total = 0
    for year, group in combined.groupby("year", sort=True):
        part_dir = table_dir / f"year={int(year)}"
        part_dir.mkdir(parents=True, exist_ok=True)
        data = group.drop(columns=["year"])
        arrow_table = pa.Table.from_pandas(
            data, schema=schema, preserve_index=False
        )
        pq.write_table(
            arrow_table, part_dir / "data.parquet", compression="snappy"
        )
        total += len(group)
        log.info(f"  Wrote {part_dir.relative_to(ROOT)}: {len(group):,} rows")
    return total


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def process_survey(survey: str, file_list: list, harmonizer) -> dict:
    """Process all years for a single survey component."""
    log.info(f"{'=' * 60}")
    log.info(f"Processing survey: {survey}")
    log.info(f"{'=' * 60}")

    frames = []
    year_stats = {}
    failed_years = []

    for year, filename in file_list:
        zip_path = download_file(filename)
        if zip_path is None:
            log.warning(f"  {filename}: download failed, skipping")
            failed_years.append(year)
            continue

        df = read_csv_from_zip(zip_path)
        if df is None:
            log.warning(f"  {filename}: CSV read failed, skipping")
            failed_years.append(year)
            continue

        # Apply harmonization
        try:
            df = harmonizer(df, year)
        except Exception as e:
            log.warning(f"  {filename}: harmonization error: {e}")
            failed_years.append(year)
            continue

        row_count = len(df)
        col_count = len(df.columns)
        log.info(f"  {filename}: {row_count:,} rows x {col_count} cols")
        year_stats[year] = {"rows": row_count, "cols": col_count}
        frames.append(df)

    if not frames:
        log.warning(f"  No data loaded for {survey}")
        return {
            "years_loaded": 0,
            "total_rows": 0,
            "failed_years": failed_years,
        }

    # Unify schemas across years
    frames = unify_schemas(frames)

    # Concatenate
    combined = pd.concat(frames, ignore_index=True)
    log.info(
        f"  Combined: {len(combined):,} rows x {len(combined.columns)} cols"
    )

    # Convert remaining object columns that look numeric
    for col in combined.columns:
        if combined[col].dtype == object:
            try:
                converted = pd.to_numeric(combined[col], errors="coerce")
                non_null_orig = combined[col].notna().sum()
                non_null_conv = converted.notna().sum()
                if (
                    non_null_orig > 0
                    and non_null_conv / max(non_null_orig, 1) > 0.7
                ):
                    combined[col] = converted
            except Exception:
                pass

    # Log column inventory
    col_types = combined.dtypes.value_counts()
    log.info(f"  Column types: {dict(col_types)}")

    # Write partitioned Parquet
    final_count = write_survey_parquet(survey, combined)
    log.info(f"  Wrote {final_count:,} rows to output/{survey}/")

    return {
        "years_loaded": len(frames),
        "total_rows": final_count,
        "columns": len(combined.columns),
        "failed_years": failed_years,
        "year_stats": year_stats,
    }


def main():
    if "--help" in sys.argv or "-h" in sys.argv:
        print(__doc__)
        print("Available tables:", ", ".join(sorted(build_manifest().keys())))
        sys.exit(0)

    manifest = build_manifest()

    # Allow filtering to specific surveys via command line
    if len(sys.argv) > 1:
        selected = set(sys.argv[1:])
        unknown = selected - set(manifest.keys())
        if unknown:
            print(f"Unknown tables: {', '.join(sorted(unknown))}")
            print("Available tables:", ", ".join(sorted(manifest.keys())))
            sys.exit(1)
        manifest = {k: v for k, v in manifest.items() if k in selected}

    survey_stats = {}

    for survey, file_list in manifest.items():
        harmonizer = HARMONIZERS.get(survey, harmonize_generic)
        stats = process_survey(survey, file_list, harmonizer)
        survey_stats[survey] = stats

    # Summary
    log.info(f"\n{'=' * 60}")
    log.info("PIPELINE SUMMARY")
    log.info(f"{'=' * 60}")

    total_rows = 0
    for survey, stats in survey_stats.items():
        yrs = stats.get("years_loaded", 0)
        rows = stats.get("total_rows", 0)
        cols = stats.get("columns", 0)
        total_rows += rows
        log.info(
            f"  {survey:15s}: {rows:>10,} rows | {cols:>4} cols | {yrs} years"
        )

    log.info(f"\n  Total rows: {total_rows:,}")

    # Log inconsistencies/notes
    for survey, stats in survey_stats.items():
        if stats.get("failed_years"):
            log.info(f"  {survey}: failed years = {stats['failed_years']}")

    for survey, stats in survey_stats.items():
        if stats.get("year_stats"):
            col_counts = [v["cols"] for v in stats["year_stats"].values()]
            if len(set(col_counts)) > 1:
                min_c, max_c = min(col_counts), max(col_counts)
                log.info(
                    f"  {survey}: column count varies {min_c}-{max_c} across years (harmonized via union)"
                )

    log.info("Done!")


if __name__ == "__main__":
    main()
