#!/usr/bin/env python3
"""
Clean the ATUS 2003-2024 multi-year microdata into hive-partitioned Parquet.

Reads the raw zips in ../input, casts each column per the architecture CSVs in
./architecture, derives `year` from the first four digits of TUCASEID, and
writes output/<table>/year=YYYY/data.parquet (snappy, explicit pyarrow schema).

atussum is reshaped wide->long (one row per case x activity code with minutes>0).
dicionario is built separately (not here).

Usage:
    uv run python models/us_bls_atus/code/clean_data.py [table ...]
"""

import csv
import logging
import sys
import zipfile
from pathlib import Path

import numpy as np
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

FILES = {
    "respondent": ("atusresp-0324.zip", "atusresp_0324.dat"),
    "roster": ("atusrost-0324.zip", "atusrost_0324.dat"),
    "cps": ("atuscps-0324.zip", "atuscps_0324.dat"),
    "activity": ("atusact-0324.zip", "atusact_0324.dat"),
    "who": ("atuswho-0324.zip", "atuswho_0324.dat"),
    "eldercare_roster": ("atusrostec-1124.zip", "atusrostec_1124.dat"),
    "case_history": ("atuscase-0524.zip", "atuscase_0524.dat"),
    "replicate_weights": ("atuswgts-0324.zip", "atuswgts_0324.dat"),
    "pandemic_replicate_weights": (
        "atuswgtspan-1920.zip",
        "atuswgtspan_1920.dat",
    ),
    "activity_summary": ("atussum-0324.zip", "atussum_0324.dat"),
}
PA = {"STRING": pa.string(), "INT64": pa.int64(), "FLOAT64": pa.float64()}


def read_arch(tbl):
    with open(ARCH / f"{tbl}.csv", newline="") as fh:
        return list(csv.DictReader(fh))


def read_dat(tbl, usecols=None):
    zf, dat = FILES[tbl]
    with zipfile.ZipFile(INPUT / zf) as z, z.open(dat) as f:
        return pd.read_csv(f, dtype=str, na_filter=False, usecols=usecols)


def cast(series, bqtype):
    if bqtype == "STRING":
        return series.where(series != "", None)
    num = pd.to_numeric(series.where(series != "", None), errors="coerce")
    return num.astype("Int64") if bqtype == "INT64" else num.astype("float64")


def clean_direct(tbl, chunksize=200_000):
    """Stream the .dat in row-chunks, casting per architecture and writing to
    per-year Parquet files (one ParquetWriter per year). Bounds memory so the
    large files (cps ~2M rows, activity ~4M rows, weights 161 cols) never OOM."""
    arch = [a for a in read_arch(tbl) if a["name"] != "year"]
    keep = set(["TUCASEID"] + [a["original_name"] for a in arch])
    schema = pa.schema(
        [pa.field(a["name"], PA[a["bigquery_type"]]) for a in arch]
    )
    order = [a["name"] for a in arch]
    zf, dat = FILES[tbl]
    writers, total = {}, 0
    with zipfile.ZipFile(INPUT / zf) as z, z.open(dat) as f:
        reader = pd.read_csv(
            f,
            dtype=str,
            na_filter=False,
            usecols=lambda c: c in keep,
            chunksize=chunksize,
        )
        for chunk in reader:
            data = {
                a["name"]: cast(
                    chunk[a["original_name"]], a["bigquery_type"]
                ).to_numpy()
                for a in arch
            }
            out = pd.DataFrame(data, columns=order)
            yr = chunk["TUCASEID"].str[:4].astype(int).to_numpy()
            for year in pd.unique(yr):
                g = out[yr == year]
                at = pa.Table.from_pandas(
                    g, schema=schema, preserve_index=False
                )
                if year not in writers:
                    pdir = OUTPUT / tbl / f"year={int(year)}"
                    pdir.mkdir(parents=True, exist_ok=True)
                    writers[year] = pq.ParquetWriter(
                        pdir / "data.parquet", schema, compression="snappy"
                    )
                writers[year].write_table(at)
                total += len(g)
    for w in writers.values():
        w.close()
    log.info(
        f"{tbl}: {total:,} rows, {len(order)} cols, {len(writers)} years -> output/{tbl}/"
    )


def clean_activity_summary():
    df = read_dat("activity_summary")
    tcols = [c for c in df.columns if c.startswith("t")]
    codes = np.array([c[1:] for c in tcols])
    year = df["TUCASEID"].str[:4].astype(int).to_numpy()
    caseid = df["TUCASEID"].to_numpy()
    vals = (
        df[tcols]
        .apply(pd.to_numeric, errors="coerce")
        .fillna(0)
        .to_numpy(dtype="int64")
    )
    ri, ci = np.nonzero(vals)  # keep only minutes != 0
    long = pd.DataFrame(
        {
            "year": year[ri],
            "tucaseid": caseid[ri],
            "activity_code": codes[ci],
            "duration_minutes": vals[ri, ci].astype("int64"),
        }
    )
    schema = pa.schema(
        [
            pa.field("tucaseid", pa.string()),
            pa.field("activity_code", pa.string()),
            pa.field("duration_minutes", pa.int64()),
        ]
    )
    order = ["tucaseid", "activity_code", "duration_minutes"]
    tdir = OUTPUT / "activity_summary"
    total = 0
    for year, g in long.groupby("year", sort=True):
        pdir = tdir / f"year={int(year)}"
        pdir.mkdir(parents=True, exist_ok=True)
        at = pa.Table.from_pandas(
            g[order], schema=schema, preserve_index=False
        )
        pq.write_table(at, pdir / "data.parquet", compression="snappy")
        total += len(g)
    log.info(
        f"activity_summary: {total:,} long rows ({len(tcols)} activity codes) -> output/activity_summary/"
    )


def main():
    OUTPUT.mkdir(exist_ok=True)
    targets = sys.argv[1:] or list(FILES.keys())
    for tbl in targets:
        log.info(f"--- {tbl} ---")
        if tbl == "activity_summary":
            clean_activity_summary()
        else:
            clean_direct(tbl)


if __name__ == "__main__":
    main()
