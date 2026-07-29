#!/usr/bin/env python3
"""Reshape the 4 county-council (Kreistag) GERDA files into long tables.

Three are vote-result files (municipality- and county-level); the fourth is a
county-year council-seat composition panel, reshaped long by party seats.

Run: cd models/us_nature_gerda/code && python3 clean_county.py
"""

import os

import gerda_common as gc
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
IN = os.path.join(HERE, "..", "input")
OUT = os.path.join(HERE, "..", "output")

CTY_FLAGS = ["flag_unsuccessful_naive_merge", "flag_total_votes_incongruent"]


def derive_cty_muni(c):
    ags = gc.norm_ags(c["ags"])
    out = pd.DataFrame(
        {
            "year": c["election_year"],
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
    if "ags_name" in c.columns:
        out["municipality_name"] = c["ags_name"]
    for f in CTY_FLAGS:
        if f in c.columns:
            out[f] = c[f]
    return out


KEEP_CTY_MUNI = [
    "year",
    "id_municipality",
    "id_county",
    "id_state",
    "municipality_name",
    "eligible_voters",
    "voters",
    "valid_votes",
    "invalid_votes",
    "turnout",
    *CTY_FLAGS,
]


def derive_cty_cty(c):
    cc = gc.norm_code(c["county_code"], 5)
    out = pd.DataFrame(
        {
            "year": c["election_year"],
            "id_county": cc,
            "id_state": cc.str.slice(0, 2),
            "eligible_voters": c["eligible_voters"],
            "voters": c["number_voters"],
            "valid_votes": c["valid_votes"],
            "invalid_votes": c["invalid_votes"],
            "turnout": c["turnout"],
        }
    )
    for f in CTY_FLAGS:
        if f in c.columns:
            out[f] = c[f]
    return out


KEEP_CTY_CTY = [
    "year",
    "id_county",
    "id_state",
    "eligible_voters",
    "voters",
    "valid_votes",
    "invalid_votes",
    "turnout",
    *CTY_FLAGS,
]

# --- county council seats panel (custom) ---
SEAT_PARTY_COLS = [
    "seats_spd",
    "seats_cdu_csu",
    "seats_fdp",
    "seats_gruene",
    "seats_freie_wahler",
    "seats_linke_pds",
    "seats_afd",
]
SEATS_KEEP = [
    "year",
    "id_county",
    "id_state",
    "county_name",
    "county_type",
    "government_party",
    "seats_total",
    "seats_regional",
    "seats_other",
    "seats_local_other",
    "flag_seats_total_incongruent",
]


def reshape_seats(path):
    df = gc.read_csv_str(path)
    cc = gc.norm_code(df["county"], 5)
    struct = pd.DataFrame(
        {
            "year": df["year"],
            "id_county": cc,
            "id_state": cc.str.slice(0, 2),
            "county_name": df.get("county_name"),
            "county_type": df.get("county_type"),
            "government_party": df.get("government_party"),
            "seats_total": df.get("seats_total"),
            "seats_regional": df.get("seats_regional"),
            "seats_other": df.get("seats_other"),
            "seats_local_other": df.get("seats_local_other"),
            "flag_seats_total_incongruent": df.get(
                "flag_seats_total_incongruent"
            ),
        }
    ).reset_index(drop=True)
    scols = [c for c in SEAT_PARTY_COLS if c in df.columns]
    sv = df[scols].reset_index(drop=True)
    n_cells = int(sv.notna().sum().sum())
    slong = sv.stack(future_stack=True).dropna().reset_index()  # noqa: PD013
    slong.columns = ["_row", "scol", "seats"]
    slong["party"] = slong["scol"].str.replace("seats_", "", regex=False)
    long = slong.join(struct, on="_row").drop(columns=["_row", "scol"])
    return long[[*SEATS_KEEP, "party", "seats"]].drop_duplicates(
        ignore_index=True
    ), n_cells


def report(name, df, n, value="vote_share"):
    key = next(c for c in ("id_municipality", "id_county") if c in df.columns)
    base = ["year", key]
    extra = ""
    if value == "vote_share":
        vs = pd.to_numeric(df[value], errors="coerce")
        q = (
            df.assign(_v=vs)
            .groupby(base)["_v"]
            .sum()
            .quantile([0.01, 0.5, 0.99])
            .round(3)
            .tolist()
        )
        extra = f"null_vs={int(vs.isna().sum())} sum_q={q}"
    print(
        f"  [{name}] rows={len(df):,} cells={n} units={df.groupby(base).ngroups:,} "
        f"parties={df['party'].nunique()} {extra}"
    )


def main():
    print("== COUNTY COUNCIL (Kreistag) — municipality level ==")
    for name, src in [
        ("county_council_municipality", "county_elec_unharm.csv"),
        (
            "county_council_municipality_harmonized_2021",
            "county_elec_harm_21_muni.csv",
        ),
    ]:
        df, n = gc.reshape_wide(
            os.path.join(IN, src), derive_cty_muni, KEEP_CTY_MUNI, []
        )
        report(name, df, n)
        gc.write_parquet(df, OUT, name)

    print("== COUNTY COUNCIL — county level ==")
    df, n = gc.reshape_wide(
        os.path.join(IN, "county_elec_harm_21_cty.csv"),
        derive_cty_cty,
        KEEP_CTY_CTY,
        [],
    )
    report("county_council_county_harmonized_2021", df, n)
    gc.write_parquet(df, OUT, "county_council_county_harmonized_2021")

    print("== COUNTY COUNCIL — seat composition panel ==")
    df, n = reshape_seats(os.path.join(IN, "county_council_seats.csv"))
    report("county_council_seats", df, n, value="seats")
    gc.write_parquet(df, OUT, "county_council_seats")
    print("done.")


if __name__ == "__main__":
    main()
