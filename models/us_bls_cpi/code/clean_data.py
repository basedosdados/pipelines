#!/usr/bin/env python3
"""
Clean the BLS CPI-U and CPI-W time.series flat files into hive-partitioned Parquet.

Reads the raw dimension + data files in ../input/{cu,cw}, resolves each series'
dimensions by joining the observations to <s>.series (authoritative), splits by
period frequency, computes percent-change columns, and writes
output/<table>/year=YYYY/data.parquet (snappy, explicit pyarrow schema).

Tables:
    monthly     period M01-M12  -> month 1-12, monthly_change, twelve_month_change
    annual      period M13, S03 -> annual average, annual_change
    semiannual  period S01, S02 -> half 1-2, semiannual_change
    dicionario  built by build_dicionario.py (not here)

Percent changes are gap-safe (self-merge on a period ordinal, not pct_change) and
computed within series (survey x seasonal x area x item). BLS does not publish them.

Usage:
    uv run python models/us_bls_cpi/code/clean_data.py [table ...]
"""

import csv
import logging
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
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("clean")

PA = {"STRING": pa.string(), "INT64": pa.int64(), "FLOAT64": pa.float64()}
SURVEYS = {"cu": "CPI-U", "cw": "CPI-W"}
KEYS = ["survey", "seasonal_adjustment", "area_id", "item_id"]


def read_arch(tbl):
    with open(ARCH / f"{tbl}.csv", newline="") as fh:
        return list(csv.DictReader(fh))


def load_series(survey_dir: str) -> pd.DataFrame:
    """<s>.series -> one row per series_id with resolved dimensions."""
    s = survey_dir
    df = pd.read_csv(
        INPUT / s / f"{s}.series",
        sep="\t",
        dtype=str,
        na_filter=False,
    )
    df.columns = [c.strip() for c in df.columns]
    out = pd.DataFrame(
        {
            "series_id": df["series_id"].str.strip(),
            "survey": SURVEYS[s],
            "seasonal_adjustment": df["seasonal"].str.strip(),
            "area_id": df["area_code"].str.strip(),
            "item_id": df["item_code"].str.strip(),
            "base_period": df["base_period"].str.strip(),
        }
    )
    return out


def load_data(survey_dir: str) -> pd.DataFrame:
    """All <s>.data.1..20 files -> stripped observations. Skips data.0.Current
    (a recent-only subset already contained in the by-group history files)."""
    s = survey_dir
    frames = []
    for path in sorted((INPUT / s).glob(f"{s}.data.*")):
        if path.name.endswith(".0.Current"):
            continue
        d = pd.read_csv(path, sep="\t", dtype=str, na_filter=False)
        d.columns = [c.strip() for c in d.columns]
        frames.append(d[["series_id", "year", "period", "value"]])
    df = pd.concat(frames, ignore_index=True)
    df["series_id"] = df["series_id"].str.strip()
    df["period"] = df["period"].str.strip()
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("int64")
    # "-" and blanks are BLS missing markers -> NaN
    df["index_value"] = pd.to_numeric(
        df["value"].str.strip().replace({"-": ""}).replace({"": None}),
        errors="coerce",
    )
    return df[["series_id", "year", "period", "index_value"]]


def build_long() -> pd.DataFrame:
    series = pd.concat([load_series(s) for s in SURVEYS], ignore_index=True)
    data = pd.concat([load_data(s) for s in SURVEYS], ignore_index=True)
    # The by-group data files cross-list ~1,400 series (identical values), so a
    # given (series_id, year, period) can repeat. Dedup to keep one observation.
    n0 = len(data)
    data = data.drop_duplicates(
        ["series_id", "year", "period"], ignore_index=True
    )
    log.info(
        f"raw observations: {n0:,}; after dedup: {len(data):,}; series: {len(series):,}"
    )
    df = data.merge(series, on="series_id", how="left", validate="many_to_one")
    missing = df["survey"].isna().sum()
    if missing:
        log.warning(f"{missing:,} rows had no series metadata (dropped)")
        df = df[df["survey"].notna()]
    for c in [*KEYS, "base_period"]:
        df[c] = df[c].astype("category")
    return df


def add_change(df, ordinal, lag, name):
    """Percent change vs the observation `lag` ordinal-steps earlier, per series."""
    prev = df[["_key", ordinal, "index_value"]].copy()
    prev[ordinal] = prev[ordinal] + lag
    prev = prev.rename(columns={"index_value": "_prev"})
    df = df.merge(prev, on=["_key", ordinal], how="left")
    df[name] = ((df["index_value"] / df["_prev"] - 1.0) * 100).round(4)
    return df.drop(columns=["_prev"])


def build_table(df, tbl):
    p = df["period"]
    if tbl == "monthly":
        sub = df[p.str.startswith("M") & (p != "M13")].copy()
        sub["month"] = sub["period"].str.slice(1).astype("int64")
        sub["_key"] = sub[KEYS].astype(str).agg("|".join, axis=1)
        sub["_t"] = sub["year"] * 12 + (sub["month"] - 1)
        sub = add_change(sub, "_t", 1, "monthly_change")
        sub = add_change(sub, "_t", 12, "twelve_month_change")
    elif tbl == "annual":
        # M13 = annual average of monthly (R) series. S03 (annual average of
        # semiannual (S) series) is intentionally excluded: it collides on the
        # natural key with M13 for areas that have both a monthly and a
        # semiannual series, and it is just the mean of the two halves already
        # present in the semiannual table.
        sub = df[p == "M13"].copy()
        sub["_key"] = sub[KEYS].astype(str).agg("|".join, axis=1)
        sub["_t"] = sub["year"]
        sub = add_change(sub, "_t", 1, "annual_change")
    elif tbl == "semiannual":
        sub = df[(p == "S01") | (p == "S02")].copy()
        sub["half"] = sub["period"].str.slice(2).astype("int64")
        sub["_key"] = sub[KEYS].astype(str).agg("|".join, axis=1)
        sub["_t"] = sub["year"] * 2 + (sub["half"] - 1)
        sub = add_change(sub, "_t", 1, "semiannual_change")
    else:
        raise ValueError(tbl)
    return sub


def write_partitioned(sub, tbl):
    arch = read_arch(tbl)
    order = [a["name"] for a in arch]
    schema = pa.schema(
        [pa.field(a["name"], PA[a["bigquery_type"]]) for a in arch]
    )
    for c in [*KEYS, "base_period"]:
        if c in sub:
            sub[c] = sub[c].astype("object")
    out = sub[order]
    total = 0
    for year, g in out.groupby("year", sort=True):
        pdir = OUTPUT / tbl / f"year={int(year)}"
        pdir.mkdir(parents=True, exist_ok=True)
        at = pa.Table.from_pandas(g, schema=schema, preserve_index=False)
        pq.write_table(at, pdir / "data.parquet", compression="snappy")
        total += len(g)
    log.info(
        f"{tbl}: {total:,} rows, {len(order)} cols, {out['year'].nunique()} years -> output/{tbl}/"
    )
    return total


def main():
    want = set(sys.argv[1:]) or {"monthly", "annual", "semiannual"}
    df = build_long()
    for tbl in ("monthly", "annual", "semiannual"):
        if tbl in want:
            write_partitioned(build_table(df, tbl), tbl)


if __name__ == "__main__":
    main()
