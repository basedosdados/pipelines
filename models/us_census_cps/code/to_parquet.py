#!/usr/bin/env python3
"""Phase-A step 4a final: convert the validated Stata extracts to partitioned Parquet.

This output IS the golden fixture the Phase-B Python port must reproduce exactly.

Layout follows bigquery-conventions.md:
    output/<table>/year=<YYY>/data.parquet                 (march: annual)
    output/<table>/year=<YYYY>/month=<M>/data.parquet      (org, basic_monthly: monthly)

Types follow the architecture CSVs (the single source of truth): INT64/FLOAT64/STRING per
`bigquery_type`, enforced via an explicit pyarrow schema so partitions can't drift.

Usage: python3 to_parquet.py [table ...]      (default: all three)
"""

import csv
import os
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# architecture CSVs sit next to this script (models/us_census_cps/code/architecture);
# the Stata build tree lives outside the repo -- override with CPS_BUILD if needed.
HERE = os.path.dirname(os.path.abspath(__file__))
ARCH = os.path.join(HERE, "architecture")
BUILD = os.environ.get(
    "CPS_BUILD", os.path.join(os.path.expanduser("~"), "cps_build")
)
OUT = f"{BUILD}/parquet"

SPECS = {
    "org": dict(
        src=lambda y: f"{BUILD}/CPS_ORG/CEPR/temp/cepr_org_{y}.dta",
        years=range(1979, 2020),
        monthly=True,
        arch="org.csv",
    ),
    "basic_monthly": dict(
        src=lambda y: (
            f"{BUILD}/CPS_Basic/CEPR/Working/cepr_basic_monthly_{y}.dta"
        ),
        years=range(1994, 2020),
        monthly=True,
        arch="basic_monthly.csv",
    ),
    # march 2014 ships as two disjoint samples: 5/8 traditional (research=0) and
    # 3/8 redesign (research=1). Same 477-col schema/order, no key overlap -> both loaded,
    # distinguished by the `research` flag.
    "march": dict(
        src=lambda y: f"{BUILD}/CPS_March/CEPR/cepr_march_{y}.dta",
        extra={2014: f"{BUILD}/CPS_March/CEPR/cepr_march_2014_research.dta"},
        years=range(2014, 2019),
        monthly=False,
        arch="march.csv",
    ),
}
PA = {"INT64": pa.int64(), "FLOAT64": pa.float64(), "STRING": pa.string()}


def load_arch(name):
    with open(f"{ARCH}/{name}") as fh:
        rows = list(csv.DictReader(fh))
    return [(r["name"], r["bigquery_type"]) for r in rows]


def cast(df, arch, drop_parts):
    """Coerce to architecture types; build an explicit pyarrow schema."""
    fields, cols = [], {}
    for name, bqt in arch:
        if name in drop_parts:  # partition keys live in the path
            continue
        s = df[name] if name in df.columns else pd.Series([None] * len(df))
        if bqt == "STRING":
            v = s.astype("string")
            # Stata stores coded categoricals as floats: 3.0 -> "3"
            if s.dtype.kind in "if":
                v = s.map(
                    lambda x: (
                        None
                        if pd.isna(x)
                        else (str(int(x)) if float(x).is_integer() else str(x))
                    )
                ).astype("string")
        elif bqt == "INT64":
            v = pd.to_numeric(s, errors="coerce").astype("Int64")
        else:
            v = pd.to_numeric(s, errors="coerce").astype("float64")
        cols[name] = v
        fields.append(pa.field(name, PA[bqt]))
    return pa.Table.from_pandas(
        pd.DataFrame(cols), schema=pa.schema(fields), preserve_index=False
    )


def convert(table):
    spec = SPECS[table]
    arch = load_arch(spec["arch"])
    parts = ["year", "month"] if spec["monthly"] else ["year"]
    total = 0
    for y in spec["years"]:
        f = spec["src"](y)
        if not os.path.exists(f):
            print(f"  {table} {y}: SOURCE MISSING", flush=True)
            continue
        df = pd.read_stata(f, convert_categoricals=False)
        ex = spec.get("extra", {}).get(y)
        if ex and os.path.exists(ex):
            add = pd.read_stata(ex, convert_categoricals=False)
            assert list(add.columns) == list(df.columns), (
                f"{table} {y}: extra-source schema mismatch"
            )
            df = pd.concat([df, add], ignore_index=True)
            print(
                f"  {table} {y}: + {len(add):,} rows from {os.path.basename(ex)}",
                flush=True,
            )
        if spec["monthly"]:
            groups = df.groupby(
                pd.to_numeric(df["month"], errors="coerce").astype("Int64")
            )
            for m, g in groups:
                d = f"{OUT}/{table}/year={y}/month={int(m)}"
                os.makedirs(d, exist_ok=True)
                pq.write_table(
                    cast(g, arch, parts),
                    f"{d}/data.parquet",
                    compression="snappy",
                )
                total += len(g)
        else:
            d = f"{OUT}/{table}/year={y}"
            os.makedirs(d, exist_ok=True)
            pq.write_table(
                cast(df, arch, parts),
                f"{d}/data.parquet",
                compression="snappy",
            )
            total += len(df)
        print(f"  {table} {y}: {len(df):>9,} rows", flush=True)
    print(f"{table}: TOTAL {total:,} rows\n", flush=True)
    return total


if __name__ == "__main__":
    targets = sys.argv[1:] or list(SPECS)
    grand = {t: convert(t) for t in targets}
    print("==== PARQUET SUMMARY ====")
    for t, n in grand.items():
        print(f"  {t}: {n:,} rows")
