#!/usr/bin/env python3
"""QA the cleaned au_abs_cpi output before upload."""

import glob
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def load(table):
    files = glob.glob(str(ROOT / "output" / table / "year=*/data.parquet"))
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    # parquet is all-STRING; cast for checks
    for c in ("year",):
        df[c] = df[c].astype(int)
    pcol = "quarter" if table == "quarterly" else "month"
    df[pcol] = df[pcol].astype(int)
    for c in (
        "index_number",
        "percentage_change_period",
        "percentage_change_year",
    ):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df, pcol


for table in ("quarterly", "monthly"):
    df, pcol = load(table)
    print("=" * 80)
    print(
        f"{table.upper()}  rows={len(df):,}  years {df.year.min()}-{df.year.max()}"
    )
    # key uniqueness
    d1 = df.duplicated(["year", pcol, "region", "index_name"]).sum()
    d2 = df.duplicated(["year", pcol, "serie_id"]).sum()
    print(
        f"  dup (year,{pcol},region,index_name) = {d1} | dup (year,{pcol},serie_id) = {d2}"
    )
    print(f"  regions: {sorted(df.region.unique())}")
    # nulls
    print(f"  null index_number = {df.index_number.isna().sum()}")
    print(
        f"  null pct_period = {df.percentage_change_period.isna().sum()} "
        f"| null pct_year = {df.percentage_change_year.isna().sum()}"
    )
    # base period: where All groups CPI, Australia, index == 100
    ag = df[
        (df.index_name == "All groups CPI") & (df.region == "Australia")
    ].copy()
    base = ag[(ag.index_number.round(1) == 100.0)]
    if len(base):
        b = base.sort_values(["year", pcol]).iloc[0]
        print(
            f"  base (All groups Australia index=100.0): {int(b.year)}-{int(b[pcol])}"
        )
    # latest headline
    latest = ag.sort_values(["year", pcol]).iloc[-1]
    print(
        f"  latest All groups Australia: {int(latest.year)}-{int(latest[pcol])} "
        f"index={latest.index_number} "
        f"chg_period={latest.percentage_change_period:.2f} "
        f"chg_year={latest.percentage_change_year:.2f}"
    )

# QA: recompute year change from index for All groups Australia and compare to stored
print("=" * 80)
print("QA: computed-vs-stored YoY for All groups Australia (monthly)")
df, pcol = load("monthly")
ag = df[
    (df.index_name == "All groups CPI") & (df.region == "Australia")
].copy()
ag = ag.sort_values(["year", "month"]).reset_index(drop=True)
ag["recomputed_year"] = (ag.index_number / ag.index_number.shift(12) - 1) * 100
cmp = ag.dropna(subset=["percentage_change_year", "recomputed_year"])
maxdiff = (cmp.percentage_change_year - cmp.recomputed_year).abs().max()
print(
    f"  n compared = {len(cmp)} | max |stored - recomputed| = {maxdiff:.4f} pp"
)
print(
    ag[
        [
            "year",
            "month",
            "index_number",
            "percentage_change_period",
            "percentage_change_year",
        ]
    ]
    .tail(6)
    .to_string(index=False)
)
