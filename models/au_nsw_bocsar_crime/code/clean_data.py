#!/usr/bin/env python3
"""Clean the BOCSAR NSW crime files in ../input into partitioned parquet in ../output.

One dataset (au_nsw_bocsar_crime), 11 tables. Staging parquet is all-STRING, hive-
partitioned by year (Data Basis convention: the dbt model safe_casts every
column; see write_partitioned). English column names per data-basis-style.md.

Usage:
    uv run --with pandas --with openpyxl --with polars python \
        models/au_nsw_bocsar_crime/code/clean_data.py [table ...]
"""

import csv
import logging
import re
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
INPUT = ROOT / "input"
OUTPUT = ROOT / "output"
ARCH = ROOT / "code" / "architecture"

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("bocsar")

PA_TYPED = {
    "INT64": pa.int64(),
    "FLOAT64": pa.float64(),
    "STRING": pa.string(),
    "DATE": pa.string(),
}
STAR = re.compile(r"[\*\^~]+")


def read_arch(table):
    with open(ARCH / f"{table}.csv", newline="") as fh:
        return list(csv.DictReader(fh))


def clean_label(s):
    """Strip footnote markers (* ^ ~) and collapse whitespace on a label."""
    if pd.isna(s):
        return s
    return re.sub(r"\s+", " ", STAR.sub("", str(s))).strip()


def fill_subcategory(df):
    """The 13 single-level offence categories carry an empty Subcategory in the
    source; fill it with the category so offence_subcategory is never null and
    the RCI tables agree with the daily table (which uses the category as leaf)."""
    sub = df["offence_subcategory"]
    df["offence_subcategory"] = sub.where(
        sub.notna() & (sub.astype(str).str.strip() != ""),
        df["offence_category"],
    )
    return df


def write_partitioned(df: pd.DataFrame, table: str) -> Path:
    """Write df as all-STRING Snappy parquet, hive-partitioned by year."""
    arch = read_arch(table)
    order = [a["name"] for a in arch]
    typed = pa.schema(
        [pa.field(a["name"], PA_TYPED[a["bigquery_type"]]) for a in arch]
    )
    string_schema = pa.schema([pa.field(a["name"], pa.string()) for a in arch])
    missing = [c for c in order if c not in df.columns]
    if missing:
        raise ValueError(f"{table}: missing columns {missing}")
    out = df[order].copy()
    # normalise numeric columns to nullable ints/floats so they serialise cleanly
    for a in arch:
        if a["bigquery_type"] == "INT64":
            out[a["name"]] = pd.array(
                pd.to_numeric(out[a["name"]], errors="coerce"), dtype="Int64"
            )
        elif a["bigquery_type"] == "FLOAT64":
            out[a["name"]] = pd.to_numeric(
                out[a["name"]], errors="coerce"
            ).astype("Float64")
        else:  # STRING / DATE already string-formatted
            out[a["name"]] = out[a["name"]].astype("object")
    tdir = OUTPUT / table
    total = 0
    for year, g in out.groupby("year", sort=True):
        # pyrefly: ignore [bad-argument-type]
        pdir = tdir / f"year={int(year)}"
        pdir.mkdir(parents=True, exist_ok=True)
        at = pa.Table.from_pandas(g, schema=typed, preserve_index=False).cast(
            string_schema
        )
        pq.write_table(at, pdir / "data.parquet", compression="snappy")
        total += at.num_rows
    log.info(
        "%s: wrote %s rows across %s year-partitions",
        table,
        f"{total:,}",
        out["year"].nunique(),
    )
    return tdir


# ----------------------------------------------------------------------------- RCI
def _melt_excel_rci(path, sheet, geo_orig, geo_new, drop_cols, engine=None):
    df = pd.read_excel(path, sheet_name=sheet, engine=engine)
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])
    id_cols = ["Offence category", "Subcategory"] + (
        [geo_orig] if geo_orig else []
    )
    month_cols = [c for c in df.columns if c not in id_cols]
    long = df.melt(
        id_vars=id_cols,
        value_vars=month_cols,
        var_name="ml",
        value_name="incidents",
    )
    lab = pd.to_datetime(long["ml"], errors="coerce")
    long = long[lab.notna()].copy()
    long["year"] = lab[lab.notna()].dt.year
    long["month"] = lab[lab.notna()].dt.month
    long["offence_category"] = long["Offence category"].map(clean_label)
    long["offence_subcategory"] = long["Subcategory"].map(clean_label)
    long = fill_subcategory(long)
    if geo_orig:
        long[geo_new] = long[geo_orig].map(clean_label)
    return long


def clean_nsw():
    long = _melt_excel_rci(
        INPUT / "Incident_by_NSW.xlsx",
        "Data",
        None,
        None,
        drop_cols=["State", "2025 population", "2026 population"],
    )
    return long[
        [
            "year",
            "month",
            "offence_category",
            "offence_subcategory",
            "incidents",
        ]
    ]


def clean_sa4():
    long = _melt_excel_rci(
        INPUT / "Statistical_Area_Monthly_Data.xlsx",
        "SA4",
        "NSW Statistical Area",
        "sa4_name",
        drop_cols=[],
    )
    return long[
        [
            "year",
            "month",
            "sa4_name",
            "offence_category",
            "offence_subcategory",
            "incidents",
        ]
    ]


def clean_lga():
    long = _melt_excel_rci(
        INPUT / "RCI_offencebymonth.xlsm",
        "Data",
        "LGA",
        "lga_name",
        drop_cols=[],
        engine="openpyxl",
    )
    return long[
        [
            "year",
            "month",
            "lga_name",
            "offence_category",
            "offence_subcategory",
            "incidents",
        ]
    ]


def _melt_csv_rci_by_year(path, table, geo_orig, geo_new):
    """Memory-bounded melt of a wide RCI CSV (postcode/suburb): read once with
    polars, unpivot one year of month-columns at a time, write each year's parquet."""
    import polars as pl

    arch = read_arch(table)
    order = [a["name"] for a in arch]
    typed = pa.schema(
        [pa.field(a["name"], PA_TYPED[a["bigquery_type"]]) for a in arch]
    )
    string_schema = pa.schema([pa.field(a["name"], pa.string()) for a in arch])
    ids = [geo_orig, "Offence category", "Subcategory"]
    df = pl.read_csv(
        path, schema_overrides={c: pl.Utf8 for c in ids}, infer_schema_length=0
    )
    month_cols = [c for c in df.columns if c not in ids]
    mon_to_num = {
        "Jan": 1,
        "Feb": 2,
        "Mar": 3,
        "Apr": 4,
        "May": 5,
        "Jun": 6,
        "Jul": 7,
        "Aug": 8,
        "Sep": 9,
        "Oct": 10,
        "Nov": 11,
        "Dec": 12,
    }
    by_year = {}
    for c in month_cols:
        mon, yr = c.split()
        by_year.setdefault(int(yr), []).append((c, mon_to_num[mon]))
    tdir = OUTPUT / table
    total = 0
    for year in sorted(by_year):
        cols = by_year[year]
        sub = df.select(ids + [c for c, _ in cols])
        long = sub.melt(
            id_vars=ids,
            value_vars=[c for c, _ in cols],
            variable_name="ml",
            value_name="incidents",
        ).to_pandas()
        m = {c: mm for c, mm in cols}
        long["year"] = year
        long["month"] = long["ml"].map(m)
        long[geo_new] = long[geo_orig].map(clean_label)
        long["offence_category"] = long["Offence category"].map(clean_label)
        long["offence_subcategory"] = long["Subcategory"].map(clean_label)
        long = fill_subcategory(long)
        g = long[order].copy()
        g["incidents"] = pd.array(
            pd.to_numeric(g["incidents"], errors="coerce"), dtype="Int64"
        )
        for col in [geo_new, "offence_category", "offence_subcategory"]:
            g[col] = g[col].astype("object")
        pdir = tdir / f"year={year}"
        pdir.mkdir(parents=True, exist_ok=True)
        at = pa.Table.from_pandas(g, schema=typed, preserve_index=False).cast(
            string_schema
        )
        pq.write_table(at, pdir / "data.parquet", compression="snappy")
        total += at.num_rows
        log.info("  %s year=%s: %s rows", table, year, f"{at.num_rows:,}")
    log.info(
        "%s: wrote %s rows across %s year-partitions",
        table,
        f"{total:,}",
        len(by_year),
    )
    return total


def clean_postcode():
    return _melt_csv_rci_by_year(
        INPUT / "PostcodeData.csv",
        "criminal_incidents_postcode",
        "Postcode",
        "postcode",
    )


def clean_suburb():
    csvs = list(INPUT.glob("SuburbData*.csv"))
    if not csvs:
        raise FileNotFoundError("SuburbData*.csv not found in input/")
    return _melt_csv_rci_by_year(
        csvs[0], "criminal_incidents_suburb", "Suburb", "suburb"
    )


# ----------------------------------------------------------------------------- daily
def _offence_category_map():
    nsw = pd.read_excel(INPUT / "Incident_by_NSW.xlsx", sheet_name="Data")
    cat = nsw["Offence category"].map(clean_label)
    sub = nsw["Subcategory"].map(clean_label)
    sub_to_cats = {}
    for cc, ss in zip(cat, sub, strict=False):
        sub_to_cats.setdefault(ss, set()).add(cc)
    cats = set(cat)

    def m(leaf):
        leaf = clean_label(leaf)
        if leaf in sub_to_cats and len(sub_to_cats[leaf]) == 1:
            return next(iter(sub_to_cats[leaf])), leaf
        if leaf in cats:
            return leaf, leaf
        return None, leaf

    return m


def clean_daily():
    df = pd.read_excel(
        INPUT / "NSW_criminal_incidents_daily.xlsx",
        sheet_name="Offence daily",
        header=3,
    )
    df = df.rename(columns={df.columns[0]: "Date"})
    date = pd.to_datetime(df["Date"], format="%d%b%Y", errors="coerce")
    df = df[date.notna()].copy()
    df["date"] = date[date.notna()]
    off_cols = [c for c in df.columns if c not in ("Date", "date")]
    long = df.melt(
        id_vars=["date"],
        value_vars=off_cols,
        var_name="offence",
        value_name="incidents",
    )
    mp = _offence_category_map()
    mapped = long["offence"].map(mp)
    long["offence_category"] = [x[0] for x in mapped]
    long["offence_subcategory"] = [x[1] for x in mapped]
    n_null = long["offence_category"].isna().sum()
    if n_null:
        log.warning(
            "daily: %s rows with unmapped offence_category (%s distinct leaves)",
            f"{n_null:,}",
            long.loc[long["offence_category"].isna(), "offence"].nunique(),
        )
    long["year"] = long["date"].dt.year
    long["date"] = long["date"].dt.strftime("%Y-%m-%d")
    return long[
        [
            "year",
            "date",
            "offence_category",
            "offence_subcategory",
            "incidents",
        ]
    ]


# ----------------------------------------------------------------------------- alleged offenders
def clean_alleged():
    df = pd.read_excel(
        INPUT / "NSW_Alleged_offender_data.xlsx",
        sheet_name="Table 1 ",
        header=6,
    )
    df = df.rename(
        columns={
            df.columns[0]: "cat",
            df.columns[1]: "sub",
            df.columns[2]: "age",
            df.columns[3]: "method",
            df.columns[4]: "detmethod",
        }
    )
    fy_cols = [
        c
        for c in df.columns
        if isinstance(c, str) and re.match(r"Jul \d{4} - Jun \d{4}", c)
    ]
    df = df[df["cat"].notna()].copy()
    df = df[df["method"].astype(str).str.strip().str.lower() != "total"]
    long = df.melt(
        id_vars=["cat", "sub", "age", "method", "detmethod"],
        value_vars=fy_cols,
        var_name="financial_year",
        value_name="poi_count",
    )
    long = long[long["poi_count"].notna()].copy()
    long["year"] = (
        long["financial_year"].str.extract(r"Jul (\d{4})").astype(int)
    )
    long["financial_year"] = long["financial_year"].str.replace(
        r"Jul (\d{4}) - Jun (\d{2})(\d{2})", r"\1-\3", regex=True
    )
    long["offence_category"] = long["cat"].map(clean_label)
    long["offence_subcategory"] = long["sub"].map(clean_label)
    long["age_group"] = long["age"].map(clean_label)
    long["legal_proceeding"] = long["method"].map(clean_label)
    long["detailed_legal_proceeding"] = long["detmethod"].map(clean_label)
    return long[
        [
            "year",
            "financial_year",
            "offence_category",
            "offence_subcategory",
            "age_group",
            "legal_proceeding",
            "detailed_legal_proceeding",
            "poi_count",
        ]
    ]


# ----------------------------------------------------------------------------- custody
def _custody(sheet, rename, value_col):
    frames = []
    for system, fname in [
        ("adult", "Adult_Custody_Report.xlsx"),
        ("youth", "Youth_Custody_Report.xlsx"),
    ]:
        d = pd.read_excel(INPUT / fname, sheet_name=sheet)
        d = d.rename(columns=rename)
        d["custody_system"] = system
        d["Month"] = pd.to_datetime(d["Month"], errors="coerce")
        d = d[d["Month"].notna()].copy()
        d["year"] = d["Month"].dt.year
        d["month"] = d["Month"].dt.month
        d["sex"] = d["sex"].map(clean_label)
        d["aboriginality"] = d["aboriginality"].map(clean_label)
        frames.append(d)
    return pd.concat(frames, ignore_index=True)


def clean_custody_population():
    d = _custody(
        "stock_data",
        {
            "Legal Status": "legal_status",
            "Aboriginality": "aboriginality",
            "Gender": "sex",
            "MSO": "most_serious_offence",
            "Count": "people",
        },
        "people",
    )
    return d[
        [
            "year",
            "month",
            "custody_system",
            "legal_status",
            "aboriginality",
            "sex",
            "most_serious_offence",
            "people",
        ]
    ]


def clean_custody_receptions():
    d = _custody(
        "reception_data",
        {
            "Reception Status": "reception_status",
            "Aboriginality": "aboriginality",
            "Gender": "sex",
            "Count": "receptions",
        },
        "receptions",
    )
    return d[
        [
            "year",
            "month",
            "custody_system",
            "reception_status",
            "aboriginality",
            "sex",
            "receptions",
        ]
    ]


def clean_custody_discharges():
    d = _custody(
        "discharge_data",
        {
            "Discharge Type": "discharge_type",
            "Discharge Type Breakdown": "discharge_type_breakdown",
            "Aboriginality": "aboriginality",
            "Gender": "sex",
            "Count": "discharges",
        },
        "discharges",
    )
    return d[
        [
            "year",
            "month",
            "custody_system",
            "discharge_type",
            "discharge_type_breakdown",
            "aboriginality",
            "sex",
            "discharges",
        ]
    ]


def clean_custody_remand_to_sentenced():
    d = _custody(
        "remand_to_sentenced_data",
        {
            "Discharge Type": "discharge_type",
            "Aboriginality": "aboriginality",
            "Gender": "sex",
            "Count": "transitions",
        },
        "transitions",
    )
    return d[
        [
            "year",
            "month",
            "custody_system",
            "discharge_type",
            "aboriginality",
            "sex",
            "transitions",
        ]
    ]


# ----------------------------------------------------------------------------- driver
BUILDERS = {
    "criminal_incidents": clean_nsw,
    "criminal_incidents_sa4": clean_sa4,
    "criminal_incidents_lga": clean_lga,
    "criminal_incidents_daily": clean_daily,
    "alleged_offenders": clean_alleged,
    "custody_population": clean_custody_population,
    "custody_receptions": clean_custody_receptions,
    "custody_discharges": clean_custody_discharges,
    "custody_remand_to_sentenced": clean_custody_remand_to_sentenced,
}
# these write their own parquet (memory-bounded) and are not in BUILDERS
SELF_WRITERS = {
    "criminal_incidents_postcode": clean_postcode,
    "criminal_incidents_suburb": clean_suburb,
}


def main():
    OUTPUT.mkdir(parents=True, exist_ok=True)
    only = set(sys.argv[1:])
    tables = list(BUILDERS) + list(SELF_WRITERS)
    if only:
        tables = [t for t in tables if t in only]
    for t in tables:
        log.info("=== %s ===", t)
        if t in SELF_WRITERS:
            SELF_WRITERS[t]()
        else:
            write_partitioned(BUILDERS[t](), t)
    log.info("DONE")


if __name__ == "__main__":
    main()
