#!/usr/bin/env python3
"""Reshape the 3 municipal (Gemeinderat) GERDA files into long tables.

Municipal is a custom reshape: besides party vote_share, each of the ten major
parties carries a per-party council-seat count (seats_<party>). Seats are melted
onto the party rows. The per-party zero->NA recode flag (replaced_0_with_na_*)
is NOT carried: by construction it is only set on rows GERDA recoded to NA, which
have no vote_share and therefore no long row; on surviving rows it is uniformly 0.

Run: cd models/us_nature_gerda/code && python3 clean_municipal.py
"""

import os

# pyrefly: ignore [missing-import]
import gerda_common as gc
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
IN = os.path.join(HERE, "..", "input")
OUT = os.path.join(HERE, "..", "output")

KEEP = [
    "year",
    "election_date",
    "id_municipality",
    "id_county",
    "id_state",
    "election_type",
    "eligible_voters",
    "voters",
    "valid_votes",
    "turnout",
    "flag_unsuccessful_naive_merge",
]


def reshape_municipal(path):
    df = gc.read_csv_str(path)
    ags = gc.norm_ags(df["ags"])
    struct = pd.DataFrame(
        {
            "year": df["election_year"],
            "election_date": df["election_date"],
            "id_municipality": ags,
            "id_county": ags.str.slice(0, 5),
            "id_state": ags.str.slice(0, 2),
            "election_type": df["election_type"]
            if "election_type" in df.columns
            else None,
            "eligible_voters": df["eligible_voters"],
            "voters": df["number_voters"],
            "valid_votes": df["valid_votes"],
            "turnout": df["turnout"],
        }
    )
    if "flag_unsuccessful_naive_merge" in df.columns:
        struct["flag_unsuccessful_naive_merge"] = df[
            "flag_unsuccessful_naive_merge"
        ]
    struct = struct.reset_index(drop=True)

    pcols = gc.party_columns(df.columns)  # 10 majors + other
    pv = df[pcols].reset_index(drop=True)
    n_cells = int(pv.notna().sum().sum())
    vlong = pv.stack(future_stack=True).dropna().reset_index()  # noqa: PD013
    vlong.columns = ["_row", "party", "vote_share"]
    long = vlong.join(struct, on="_row")

    scols = [c for c in df.columns if c.startswith("seats_")]
    if scols:
        sv = df[scols].reset_index(drop=True)
        slong = sv.stack(future_stack=True).dropna().reset_index()  # noqa: PD013
        slong.columns = ["_row", "scol", "seats"]
        slong["party"] = slong["scol"].str.replace("seats_", "", regex=False)
        long = long.merge(
            slong[["_row", "party", "seats"]], on=["_row", "party"], how="left"
        )
    else:
        long["seats"] = None

    long = long.drop(columns="_row")
    cols = [c for c in KEEP if c in long.columns] + [
        "party",
        "vote_share",
        "seats",
    ]
    return long[cols].drop_duplicates(ignore_index=True), n_cells


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
    seats_n = int(pd.to_numeric(df["seats"], errors="coerce").notna().sum())
    print(
        f"  [{name}] rows={len(df):,} cells={n} units={df.groupby(base).ngroups:,} "
        f"parties={df['party'].nunique()} null_vs={int(vs.isna().sum())} "
        f"seats_rows={seats_n} sum_q={q}"
    )


def main():
    print("== MUNICIPAL (Gemeinderat) ==")
    for name, src in [
        ("municipal", "municipal_unharm.csv"),
        ("municipal_harmonized_2021", "municipal_harm.csv"),
        ("municipal_harmonized_2025", "municipal_harm_25.csv"),
    ]:
        df, n = reshape_municipal(os.path.join(IN, src))
        report(name, df, n)
        gc.write_parquet(df, OUT, name)
    print("done.")


if __name__ == "__main__":
    main()
