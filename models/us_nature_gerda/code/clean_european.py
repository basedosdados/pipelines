#!/usr/bin/env python3
"""Reshape the 2 European (Europawahl) GERDA files into long tables.

Run: cd models/us_nature_gerda/code && python3 clean_european.py
"""

import os

# pyrefly: ignore [missing-import]
import gerda_common as gc
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
IN = os.path.join(HERE, "..", "input")
OUT = os.path.join(HERE, "..", "output")

EU_FLAGS = ["flag_turnout_above_1", "flag_unsuccessful_naive_merge"]


def derive_euro(c):
    ags = gc.norm_ags(c["ags"])
    out = pd.DataFrame(
        {
            "year": c["election_year"],
            "election_date": c["election_date"],
            "id_municipality": ags,
            "id_county": ags.str.slice(0, 5),
            "id_state": ags.str.slice(0, 2),
            "eligible_voters": c["eligible_voters"],
            "voters": c["number_voters"],
            "valid_votes": c["valid_votes"],
            "invalid_votes": c["invalid_votes"],
            "turnout": c["turnout"],
        }
    )
    for f in EU_FLAGS:
        if f in c.columns:
            out[f] = c[f]
    return out


KEEP_EURO = [
    "year",
    "election_date",
    "id_municipality",
    "id_county",
    "id_state",
    "eligible_voters",
    "voters",
    "valid_votes",
    "invalid_votes",
    "turnout",
    *EU_FLAGS,
]


def report(name, df, n):
    base = ["year", "id_municipality"]
    vs = pd.to_numeric(df["vote_share"], errors="coerce")
    q = (
        df.assign(_v=vs)
        .groupby(base)["_v"]
        .sum()
        .quantile([0.01, 0.5, 0.99])
        .round(3)
        .tolist()
    )
    print(
        f"  [{name}] rows={len(df):,} cells={n} units={df.groupby(base).ngroups:,} "
        f"parties={df['party'].nunique()} null_vs={int(vs.isna().sum())} sum_q={q}"
    )


def main():
    print("== EUROPEAN municipality ==")
    for name, src in [
        ("european_municipality", "european_muni_unharm.csv"),
        ("european_municipality_harmonized_2021", "european_muni_harm.csv"),
    ]:
        df, n = gc.reshape_wide(
            os.path.join(IN, src), derive_euro, KEEP_EURO, []
        )
        report(name, df, n)
        gc.write_parquet(df, OUT, name)
    print("done.")


if __name__ == "__main__":
    main()
