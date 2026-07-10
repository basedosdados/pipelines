#!/usr/bin/env python3
"""Clean and harmonize MEDSL aggregate election returns into 3 long tables.

Stacks MEDSL's per-office / per-geography source files into geography-first
long tables (state, county, district), all offices and years stacked. Output
is one all-STRING snappy Parquet per table; the dbt models safe_cast to final
types and partition by year.

Run from the dataset code dir:
    cd models/us_medsl_elections/code && python3 clean.py
"""

import os

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
INPUT = os.path.join(HERE, "..", "input")
OUTPUT = os.path.join(HERE, "..", "output")

# ---------------------------------------------------------------- helpers ----


def read_delim(fname):
    """Read a MEDSL data file as all-strings, sniffing the delimiter from the
    header (MEDSL's `?format=original` exports are comma-separated regardless of
    the .tab extension)."""
    path = os.path.join(INPUT, fname)
    with open(path, encoding="utf-8", errors="replace") as fh:
        first = fh.readline()
    sep = "\t" if first.count("\t") > first.count(",") else ","
    try:
        df = pd.read_csv(
            path,
            sep=sep,
            dtype=str,
            keep_default_na=False,
            na_values=[],
            encoding="utf-8",
        )
    except UnicodeDecodeError:
        df = pd.read_csv(
            path,
            sep=sep,
            dtype=str,
            keep_default_na=False,
            na_values=[],
            encoding="cp1252",
        )
    df.columns = [c.strip() for c in df.columns]
    print(f"  read {fname}: {df.shape[0]:,} rows, cols={list(df.columns)}")
    return df


def norm_int(series):
    """Normalize a numeric-code/count string to a clean integer string, dropping
    float suffixes ('7310.0' -> '7310'); blanks and non-numerics become blank."""

    def one(v):
        v = str(v).strip()
        if v in ("", "nan", "NaN", "None", "NA"):
            return ""
        try:
            return str(int(float(v)))
        except (ValueError, OverflowError):
            return ""

    return series.map(one)


def zpad(series, width):
    """Zero-pad a numeric-code string column to `width`; blanks stay blank."""

    def one(v):
        v = str(v).strip()
        if v in ("", "nan", "NaN", "None", "NA"):
            return ""
        v = v.split(".")[0]  # drop any decimal suffix like '1001.0'
        return v.zfill(width)

    return series.map(one)


POSTAL2FIPS = {
    "AL": "01",
    "AK": "02",
    "AZ": "04",
    "AR": "05",
    "CA": "06",
    "CO": "08",
    "CT": "09",
    "DE": "10",
    "DC": "11",
    "FL": "12",
    "GA": "13",
    "HI": "15",
    "ID": "16",
    "IL": "17",
    "IN": "18",
    "IA": "19",
    "KS": "20",
    "KY": "21",
    "LA": "22",
    "ME": "23",
    "MD": "24",
    "MA": "25",
    "MI": "26",
    "MN": "27",
    "MS": "28",
    "MO": "29",
    "MT": "30",
    "NE": "31",
    "NV": "32",
    "NH": "33",
    "NJ": "34",
    "NM": "35",
    "NY": "36",
    "NC": "37",
    "ND": "38",
    "OH": "39",
    "OK": "40",
    "OR": "41",
    "PA": "42",
    "RI": "44",
    "SC": "45",
    "SD": "46",
    "TN": "47",
    "TX": "48",
    "UT": "49",
    "VT": "50",
    "VA": "51",
    "WA": "53",
    "WV": "54",
    "WI": "55",
    "WY": "56",
    "AS": "60",
    "GU": "66",
    "MP": "69",
    "PR": "72",
    "VI": "78",
}


def state_fips_from_abbr(abbr_series, fallback_series):
    """Resolve state FIPS from the postal abbreviation (authoritative), falling
    back to a supplied 2-digit code when the abbreviation is unknown."""

    def one(a, fb):
        a = str(a).strip().upper()
        if a in POSTAL2FIPS:
            return POSTAL2FIPS[a]
        fb = str(fb).strip()
        return fb if (len(fb) == 2 and fb.isdigit()) else ""

    return [
        one(a, fb) for a, fb in zip(abbr_series, fallback_series, strict=False)
    ]


def simplify_party(series):
    """Collapse a party label to DEMOCRAT / REPUBLICAN / LIBERTARIAN / OTHER."""

    def one(p):
        u = str(p).strip().upper()
        if u in ("", "NAN", "NONE", "NA"):
            return ""
        if "DEMOCRAT" in u:
            return "DEMOCRAT"
        if "REPUBLICAN" in u:
            return "REPUBLICAN"
        if "LIBERTARIAN" in u:
            return "LIBERTARIAN"
        return "OTHER"

    return series.map(one)


def assemble(df, mapping, table_cols):
    """Build a unified frame: mapping is {unified_col: source_col}; any unified
    column not in mapping is created empty. Returns df with exactly table_cols."""
    out = pd.DataFrame(index=df.index)
    for col in table_cols:
        src = mapping.get(col)
        if src is None:
            out[col] = ""
        else:
            out[col] = df[src].fillna("").map(lambda x: str(x).strip())
    return out[table_cols]


def finalize(df, table_cols):
    """Strip strings, blank out literal 'nan'/'NA', enforce column order."""
    for c in df.columns:
        df[c] = (
            df[c]
            .astype(str)
            .str.strip()
            .replace({"nan": "", "NaN": "", "None": "", "NA": ""})
        )
    return df[table_cols].reset_index(drop=True)


# ------------------------------------------------------------- state table ----

STATE_COLS = [
    "year",
    "id_state",
    "state_abbreviation",
    "state_census_code",
    "state_icpsr_code",
    "office",
    "district",
    "stage",
    "indicator_special",
    "candidate",
    "party_detailed",
    "party_simplified",
    "indicator_writein",
    "mode",
    "votes",
    "total_votes",
    "indicator_unofficial",
    "version",
]


def build_state():
    print("Building `state` (President + Senate + statewide state offices)...")
    frames = []

    # President 1976-2024 (statewide)
    p = read_delim("president_1976_2024.csv")
    fp = assemble(
        p,
        {
            "year": "year",
            "id_state": "state_fips",
            "state_abbreviation": "state_po",
            "state_census_code": "state_cen",
            "state_icpsr_code": "state_ic",
            "office": "office",
            "candidate": "candidate",
            "party_detailed": "party_detailed",
            "party_simplified": "party_simplified",
            "indicator_writein": "writein",
            "votes": "candidatevotes",
            "total_votes": "totalvotes",
            "version": "version",
        },
        STATE_COLS,
    )
    frames.append(fp)

    # Senate statewide 1976-2024
    s = read_delim("senate_state_1976_2024.tab")
    fs = assemble(
        s,
        {
            "year": "year",
            "id_state": "state_fips",
            "state_abbreviation": "state_po",
            "state_census_code": "state_cen",
            "state_icpsr_code": "state_ic",
            "office": "office",
            "district": "district",
            "stage": "stage",
            "indicator_special": "special",
            "candidate": "candidate",
            "party_detailed": "party_detailed",
            "party_simplified": "party_simplified",
            "indicator_writein": "writein",
            "mode": "mode",
            "votes": "candidatevotes",
            "total_votes": "totalvotes",
            "indicator_unofficial": "unofficial",
            "version": "version",
        },
        STATE_COLS,
    )
    frames.append(fs)

    # State offices 2016 (governor and other statewide offices)
    x = read_delim("state_offices_2016.tab")
    fx = assemble(
        x,
        {
            "year": "year",
            "id_state": "state_fips",
            "state_abbreviation": "state_po",
            "state_census_code": "state_cen",
            "state_icpsr_code": "state_ic",
            "office": "office",
            "district": "district",
            "stage": "stage",
            "indicator_special": "special",
            "candidate": "candidate",
            "party_detailed": "party",
            "indicator_writein": "writein",
            "mode": "mode",
            "votes": "candidatevotes",
            "total_votes": "totalvotes",
            "version": "version",
        },
        STATE_COLS,
    )
    fx["party_simplified"] = simplify_party(fx["party_detailed"])
    frames.append(fx)

    df = pd.concat(frames, ignore_index=True)
    df["id_state"] = state_fips_from_abbr(
        df["state_abbreviation"], zpad(df["id_state"], 2)
    )
    df["state_census_code"] = zpad(df["state_census_code"], 2)
    df["state_icpsr_code"] = zpad(df["state_icpsr_code"], 2)
    df["party_detailed"] = df["party_detailed"].str.upper()
    df["party_simplified"] = df["party_simplified"].str.upper()
    for c in ["year", "votes", "total_votes"]:
        df[c] = norm_int(df[c])
    df.loc[
        df["district"].str.strip().str.lower() == "statewide", "district"
    ] = ""
    df = df.sort_values(["year", "id_state", "office"]).reset_index(drop=True)
    return finalize(df, STATE_COLS)


# ------------------------------------------------------------ county table ----

COUNTY_COLS = [
    "year",
    "id_state",
    "state_abbreviation",
    "id_county",
    "county_name",
    "office",
    "candidate",
    "party_detailed",
    "party_simplified",
    "mode",
    "votes",
    "total_votes",
    "version",
]


def build_county():
    print("Building `county` (County Presidential Returns 2000-2024)...")
    c = read_delim("countypres_2000_2024.tab")
    fc = assemble(
        c,
        {
            "year": "year",
            "state_abbreviation": "state_po",
            "id_county": "county_fips",
            "county_name": "county_name",
            "office": "office",
            "candidate": "candidate",
            "party_detailed": "party",
            "mode": "mode",
            "votes": "candidatevotes",
            "total_votes": "totalvotes",
            "version": "version",
        },
        COUNTY_COLS,
    )
    fc["party_simplified"] = simplify_party(fc["party_detailed"])
    fc["id_county"] = zpad(fc["id_county"], 5)
    fc["id_state"] = state_fips_from_abbr(
        fc["state_abbreviation"], fc["id_county"].str[:2]
    )
    fc["party_detailed"] = fc["party_detailed"].str.upper()
    fc["party_simplified"] = fc["party_simplified"].str.upper()
    for col in ["year", "votes", "total_votes"]:
        fc[col] = norm_int(fc[col])
    fc = fc.sort_values(
        ["year", "id_state", "id_county", "office"]
    ).reset_index(drop=True)
    return finalize(fc, COUNTY_COLS)


# ---------------------------------------------------------- district table ----

DISTRICT_COLS = [
    "year",
    "id_state",
    "state_abbreviation",
    "state_census_code",
    "state_icpsr_code",
    "district",
    "stage",
    "indicator_runoff",
    "indicator_special",
    "candidate",
    "party_detailed",
    "party_simplified",
    "indicator_writein",
    "mode",
    "votes",
    "total_votes",
    "indicator_unofficial",
    "indicator_fusion_ticket",
    "version",
]


def build_district():
    print("Building `district` (U.S. House 1976-2024)...")
    h = read_delim("house_1976_2024.tab")
    fh = assemble(
        h,
        {
            "year": "year",
            "id_state": "state_fips",
            "state_abbreviation": "state_po",
            "state_census_code": "state_cen",
            "state_icpsr_code": "state_ic",
            "district": "district",
            "stage": "stage",
            "indicator_runoff": "runoff",
            "indicator_special": "special",
            "candidate": "candidate",
            "party_detailed": "party",
            "indicator_writein": "writein",
            "mode": "mode",
            "votes": "candidatevotes",
            "total_votes": "totalvotes",
            "indicator_unofficial": "unofficial",
            "indicator_fusion_ticket": "fusion_ticket",
            "version": "version",
        },
        DISTRICT_COLS,
    )
    fh["party_simplified"] = simplify_party(fh["party_detailed"])
    fh["id_state"] = state_fips_from_abbr(
        fh["state_abbreviation"], zpad(fh["id_state"], 2)
    )
    fh["state_census_code"] = zpad(fh["state_census_code"], 2)
    fh["state_icpsr_code"] = zpad(fh["state_icpsr_code"], 2)
    fh["district"] = zpad(fh["district"], 2)
    fh["party_detailed"] = fh["party_detailed"].str.upper()
    fh["party_simplified"] = fh["party_simplified"].str.upper()
    for c in ["year", "votes", "total_votes"]:
        fh[c] = norm_int(fh[c])
    fh = fh.sort_values(["year", "id_state", "district"]).reset_index(
        drop=True
    )
    return finalize(fh, DISTRICT_COLS)


# ----------------------------------------------------------------- driver ----


def save(df, slug):
    out_dir = os.path.join(OUTPUT, slug)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "data.parquet")
    df.to_parquet(path, engine="pyarrow", compression="snappy", index=False)
    print(
        f"  saved {slug}: {df.shape[0]:,} rows x {df.shape[1]} cols -> {path}"
    )
    print(f"  years: {sorted(df['year'].unique())}")


if __name__ == "__main__":
    os.makedirs(OUTPUT, exist_ok=True)
    save(build_state(), "state")
    save(build_county(), "county")
    save(build_district(), "district")
    print("Done.")
