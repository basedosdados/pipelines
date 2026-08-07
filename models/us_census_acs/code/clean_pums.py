#!/usr/bin/env python3
"""Clean PUMS microdata -> partitioned parquet (hybrid canonical schema).

Reads zip CSV members directly (chunked, no disk extraction), harmonizes each
vintage to the canonical column set from the architecture CSVs, casts types,
adds ano/period, writes output/<table>/ano=<y>/<period>.parquet.

Usage:
  python3 clean_pums.py [VINTAGE ...]   # e.g. 2005_1yr  (default: all vintages)
"""

import csv as csvmod
import glob
import io
import os
import sys
import zipfile

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

PUMS = "/Users/rdahis/acs_data/pums"
OUTROOT = "/Users/rdahis/acs_data/output"
ARCH = os.path.join(os.path.dirname(__file__), "architecture")
CHUNK = 200_000

# pyrefly: ignore [missing-import]
from _pums_schema import RENAME  # noqa: E402  (shared identity-rename map)


def load_arch(kind):  # kind: microdata_person / microdata_household
    with open(f"{ARCH}/{kind}.csv") as fh:
        rows = list(csvmod.DictReader(fh))
    data = [r for r in rows if r["original_name"]]  # skip ano/period
    canon_orig = [r["original_name"] for r in data]  # uppercase raw names
    name_of = {r["original_name"]: r["name"] for r in data}
    bqtype = {r["name"]: r["bigquery_type"] for r in data}
    names = [r["name"] for r in data]
    return canon_orig, name_of, bqtype, names


def pa_schema(names, bqtype):
    # ano is the hive partition (encoded in the path), NOT a column in the file
    fields = [("period", pa.string())]
    for n in names:
        fields.append((n, pa.int64() if bqtype[n] == "INT64" else pa.string()))
    return pa.schema(fields)


def csv_members(zp):
    with zipfile.ZipFile(zp) as z:
        return sorted(n for n in z.namelist() if n.lower().endswith(".csv"))


def clean_one(tag, kind, zip_name):
    period = "1-year" if tag.endswith("_1yr") else "5-year"
    ano = int(tag[:4])
    canon_orig, name_of, bqtype, names = load_arch(kind)
    schema = pa_schema(names, bqtype)
    int_cols = [n for n in names if bqtype[n] == "INT64"]
    str_cols = [n for n in names if bqtype[n] == "STRING"]

    outdir = f"{OUTROOT}/{kind}/ano={ano}"
    os.makedirs(outdir, exist_ok=True)
    outfile = f"{outdir}/{period}.parquet"
    if os.path.exists(outfile):
        print(f"  SKIP {kind} {tag} (exists)")
        return
    tmp = outfile + ".tmp"

    # Resolve the archive(s) to read. Prefer the combined national zip; if it is
    # absent, the national file may be split into parts (a-d, by state FIPS range) —
    # ALL of them must be processed, or dropping b/c/d silently yields an incomplete
    # Parquet. (Our downloads use the combined zip, which itself holds all parts as
    # members; the split-zip branch guards against a differently shaped download.)
    combined = f"{PUMS}/{tag}/{zip_name}"
    if os.path.exists(combined):
        zips = [combined]
    else:
        stem = zip_name[:-4]  # "csv_pus.zip" -> "csv_pus"
        zips = sorted(glob.glob(f"{PUMS}/{tag}/{stem}[a-d].zip"))
    if not zips:
        print(f"  !! missing zip for {kind} {tag}")
        return

    writer = pq.ParquetWriter(tmp, schema, compression="snappy")
    total = 0
    for zp in zips:
        with zipfile.ZipFile(zp) as z:
            members = [n for n in z.namelist() if n.lower().endswith(".csv")]
            for member in sorted(members):
                with z.open(member) as raw:
                    reader = pd.read_csv(
                        io.TextIOWrapper(raw, "latin-1"),
                        dtype=str,
                        na_filter=False,
                        chunksize=CHUNK,
                    )
                    for chunk in reader:
                        chunk.columns = [
                            c.strip().upper() for c in chunk.columns
                        ]
                        chunk = chunk.rename(columns=RENAME)
                        chunk = chunk.loc[:, ~chunk.columns.duplicated()]
                        chunk = chunk.reindex(columns=canon_orig)
                        chunk.columns = [name_of[c] for c in canon_orig]
                        for n in int_cols:
                            chunk[n] = pd.to_numeric(
                                chunk[n], errors="coerce"
                            ).astype("Int64")
                        for n in str_cols:
                            s = chunk[n].astype("string")
                            chunk[n] = s.where(s.str.len() > 0, other=pd.NA)
                        chunk.insert(
                            0, "period", period
                        )  # ano is path-only (partition)
                        tbl = pa.Table.from_pandas(
                            chunk, schema=schema, preserve_index=False
                        )
                        writer.write_table(tbl)
                        total += len(chunk)
    writer.close()
    os.replace(tmp, outfile)
    print(
        f"  OK   {kind} {tag}: {total:,} rows -> {outfile} "
        f"({len(zips)} archive(s))"
    )


def main():
    args = sys.argv[1:]
    if args:
        vintages = args
    else:
        vintages = [
            os.path.basename(d)
            for d in sorted(glob.glob(f"{PUMS}/*_1yr"))
            + sorted(glob.glob(f"{PUMS}/*_5yr"))
        ]
    for tag in vintages:
        print(f"[{tag}]")
        clean_one(tag, "microdata_person", "csv_pus.zip")
        clean_one(tag, "microdata_household", "csv_hus.zip")


if __name__ == "__main__":
    main()
