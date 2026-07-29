#!/usr/bin/env python3
"""Reshape the 5 state (Landtag) GERDA files into long tables.

Run: cd models/us_nature_gerda/code && python3 clean_state.py
"""

import os

import gerda_common as gc
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
IN = os.path.join(HERE, "..", "input")
OUT = os.path.join(HERE, "..", "output")

# flags kept where present (state files vary in which they ship)
STATE_FLAGS = [
    "flag_briefwahl_only",
    "flag_no_valid_votes",
    "flag_naive_turnout_above_1",
    "flag_harm_turnout_above_1",
    "flag_unsuccessful_naive_merge",
    "flag_other_party_residual",
]


def derive_state_muni(c):
    ags = gc.norm_ags(c["ags"])  # geo derived from normalized AGS
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
    for f in STATE_FLAGS:
        if f in c.columns:
            out[f] = c[f]
    return out


KEEP_STATE_MUNI = [
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
    *STATE_FLAGS,
]


def derive_ltw_wkr(d):
    st = gc.norm_code(d["state"], 2)
    return pd.DataFrame(
        {
            "year": d["election_year"],
            "election_date": d["election_date"],
            "id_constituency": "state_"
            + st
            + "_"
            + d["wkr_nr"].astype(str).str.zfill(3),
            "constituency_name": d["wkr_name"],
            "id_state": st,
            "ballot": d["stimme"].map(gc.BALLOT).fillna(d["stimme"]),
            "eligible_voters": d["eligible_voters"],
            "voters": d["number_voters"],
            "valid_votes": d["valid_votes"],
            "invalid_votes": d["invalid_votes"],
            "turnout": d["turnout"],
        }
    )


KEEP_LTW = [
    "year",
    "election_date",
    "id_constituency",
    "constituency_name",
    "id_state",
    "ballot",
    "eligible_voters",
    "voters",
    "valid_votes",
    "invalid_votes",
    "turnout",
]


def report(name, df, n):
    key = next(
        c for c in ("id_municipality", "id_constituency") if c in df.columns
    )
    base = ["year", key] + (["ballot"] if "ballot" in df.columns else [])
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
        f"  [{name}] rows={len(df):,} cells={n if n is not None else 'n/a'} "
        f"units={df.groupby(base).ngroups:,} parties={df['party'].nunique()} "
        f"null_vs={int(vs.isna().sum())} sum_q={q}"
    )


def main():
    print("== STATE municipality ==")
    for name, src in [
        ("state_municipality", "state_unharm.csv"),
        ("state_municipality_harmonized_2021", "state_harm_21.csv"),
        ("state_municipality_harmonized_2023", "state_harm_23.csv"),
        ("state_municipality_harmonized_2025", "state_harm_25.csv"),
    ]:
        df, n = gc.reshape_wide(
            os.path.join(IN, src), derive_state_muni, KEEP_STATE_MUNI, []
        )
        report(name, df, n)
        gc.write_parquet(df, OUT, name)

    print("== STATE constituency ==")
    df, _ = gc.passthrough_long(
        os.path.join(IN, "ltw_wkr_unharm_long.csv"),
        derive_ltw_wkr,
        KEEP_LTW,
        ["votes", "vote_share"],
    )
    report("state_constituency", df, None)
    gc.write_parquet(df, OUT, "state_constituency")
    print("done.")


if __name__ == "__main__":
    main()
