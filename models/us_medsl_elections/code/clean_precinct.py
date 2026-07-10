#!/usr/bin/env python3
"""Clean and harmonize MEDSL precinct-level returns (2016-2024) into one long,
normalized table.

Stacks all offices and years at the precinct level. Geography is normalized to
directory foreign keys (id_state, id_county); state/county names and code
systems are dropped (join via br_bd_diretorios_us). 2016 (five per-office
national files) is remapped to the 2018-2024 harmonized schema. Output is one
all-STRING snappy Parquet per year under output/precinct/, written incrementally
(chunked read + ParquetWriter) to bound memory on the large-state files.

Run: cd models/us_medsl_elections/code && python3 clean_precinct.py
"""

import glob
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

HERE = os.path.dirname(os.path.abspath(__file__))
INPUT = os.path.join(HERE, "..", "input", "precinct")
OUTPUT = os.path.join(HERE, "..", "output", "precinct")
CHUNK = 400_000

TARGET = [
    "year",
    "date",
    "stage",
    "id_state",
    "id_county",
    "id_jurisdiction",
    "jurisdiction_name",
    "precinct",
    "office",
    "office_category",
    "district",
    "magnitude",
    "candidate",
    "party_detailed",
    "party_simplified",
    "indicator_special",
    "indicator_writein",
    "mode",
    "votes",
    "indicator_readme_check",
]
SCHEMA = pa.schema([(c, pa.string()) for c in TARGET])
# Textual columns uppercased for cross-year consistency (2016 is mixed-case).
UPPER = {
    "office",
    "office_category",
    "district",
    "candidate",
    "party_detailed",
    "party_simplified",
    "indicator_special",
    "indicator_writein",
    "mode",
    "stage",
    "jurisdiction_name",
    "precinct",
}


def zpad(series, width):
    def one(v):
        v = str(v).strip()
        if v in ("", "nan", "NaN", "None", "NA"):
            return ""
        v = v.split(".")[0]
        return v.zfill(width)

    return series.map(one)


def norm_int(series):
    def one(v):
        v = str(v).strip()
        if v in ("", "nan", "NaN", "None", "NA", "*"):
            return ""
        try:
            return str(int(float(v)))
        except (ValueError, OverflowError):
            return ""

    return series.map(one)


def simplify_party(series):
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


def pick(df, *names):
    for n in names:
        if n in df.columns:
            return df[n].fillna("")
    return pd.Series("", index=df.index)


def harmonize(df, year, office_cat_default):
    df.columns = [c.strip().lower() for c in df.columns]
    out = pd.DataFrame(index=df.index)
    out["year"] = str(year)
    out["id_state"] = zpad(pick(df, "state_fips"), 2)
    out["id_county"] = zpad(pick(df, "county_fips"), 5)
    out["id_jurisdiction"] = (
        pick(df, "jurisdiction_fips").astype(str).str.strip()
    )
    out["jurisdiction_name"] = pick(df, "jurisdiction_name", "jurisdiction")
    out["precinct"] = pick(df, "precinct")
    out["office"] = pick(df, "office")
    dv = pick(df, "dataverse").astype(str).str.strip()
    out["office_category"] = dv.where(dv != "", office_cat_default or "")
    out["district"] = pick(df, "district")
    out["magnitude"] = norm_int(pick(df, "magnitude"))
    out["candidate"] = pick(df, "candidate")
    detailed = pick(df, "party_detailed", "party")
    out["party_detailed"] = detailed
    simp = pick(df, "party_simplified").astype(str).str.strip()
    out["party_simplified"] = simp.where(simp != "", simplify_party(detailed))
    out["indicator_special"] = pick(df, "special")
    out["indicator_writein"] = pick(df, "writein")
    out["mode"] = pick(df, "mode")
    out["votes"] = norm_int(pick(df, "votes"))
    out["stage"] = pick(df, "stage")
    out["date"] = pick(df, "date").astype(str).str.strip()
    out["indicator_readme_check"] = pick(df, "readme_check")

    for c in TARGET:
        out[c] = (
            out[c]
            .astype(str)
            .str.strip()
            .replace({"nan": "", "NaN": "", "None": "", "NA": ""})
        )
    for c in UPPER:
        out[c] = out[c].str.upper()
    return out[TARGET]


def sniff_sep(path):
    with open(path, encoding="utf-8", errors="replace") as fh:
        first = fh.readline()
    return "\t" if first.count("\t") > first.count(",") else ","


def process_year(year, files_with_cat):
    out_path = os.path.join(OUTPUT, f"{year}.parquet")
    writer = None
    total = 0
    for filepath, office_cat in files_with_cat:
        sep = sniff_sep(filepath)
        reader = pd.read_csv(
            filepath,
            sep=sep,
            dtype=str,
            keep_default_na=False,
            na_values=[],
            chunksize=CHUNK,
            encoding="utf-8",
            encoding_errors="replace",
            on_bad_lines="skip",
        )
        for chunk in reader:
            h = harmonize(chunk, year, office_cat)
            tbl = pa.Table.from_pandas(h, schema=SCHEMA, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(
                    out_path, SCHEMA, compression="snappy"
                )
            writer.write_table(tbl)
            total += len(h)
        print(f"    {os.path.basename(filepath)}: {total:,} cumulative rows")
    if writer:
        writer.close()
    print(f"  {year}: {total:,} rows -> {out_path}")
    return total


def year_files(year):
    """State files for 2018-2024 (office_category from the `dataverse` column)."""
    paths = sorted(glob.glob(os.path.join(INPUT, str(year), "*.csv")))
    return [(p, None) for p in paths]


def files_2016():
    base = os.path.join(INPUT, "2016")
    cats = {
        "president": "PRESIDENT",
        "senate": "SENATE",
        "house": "HOUSE",
        "state": "STATE",
        "local": "LOCAL",
    }
    out = []
    for name, cat in cats.items():
        p = os.path.join(base, f"{name}.csv")
        if os.path.exists(p):
            out.append((p, cat))
    return out


if __name__ == "__main__":
    os.makedirs(OUTPUT, exist_ok=True)
    grand = 0
    print("Building `precinct` (2016)...")
    grand += process_year(2016, files_2016())
    for y in (2018, 2020, 2022, 2024):
        print(f"Building `precinct` ({y})...")
        grand += process_year(y, year_files(y))
    print(f"Done. total precinct rows: {grand:,}")
