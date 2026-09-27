#!/usr/bin/env python3
"""Reshape the 7 federal (Bundestag) GERDA files into long tables.

Run: cd models/us_nature_gerda/code && python3 clean_federal.py
"""

import os

import gerda_common as gc
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
IN = os.path.join(HERE, "..", "input")
OUT = os.path.join(HERE, "..", "output")


# ----------------------------------------------------------- derive helpers ---
def derive_muni(c):
    ags = gc.norm_code(c["ags"], 8)
    out = pd.DataFrame(
        {
            "year": c["election_year"],
            "election_date": c["election_date"],
            "id_municipality": ags,
            "id_county": ags.str.slice(
                0, 5
            ),  # county = first 5 of the normalized AGS
            "id_state": ags.str.slice(
                0, 2
            ),  # state  = first 2 of the normalized AGS
            "municipality_name": c["ags_name"],
            "eligible_voters": c["eligible_voters"],
            "voters": c["number_voters"],
            "valid_votes": c["valid_votes"],
            "invalid_votes": c["invalid_votes"],
            "turnout": c["turnout"],
        }
    )
    for f in (
        "flag_naive_turnout_above_1",
        "flag_unsuccessful_naive_merge",
        "flag_total_votes_incongruent",
    ):
        if f in c.columns:
            out[f] = c[f]
    return out


KEEP_MUNI = [
    "year",
    "election_date",
    "id_municipality",
    "id_county",
    "id_state",
    "municipality_name",
    "eligible_voters",
    "voters",
    "valid_votes",
    "invalid_votes",
    "turnout",
    "flag_naive_turnout_above_1",
    "flag_unsuccessful_naive_merge",
    "flag_total_votes_incongruent",
]


def derive_county(c):
    out = pd.DataFrame(
        {
            "year": c["year"] if "year" in c.columns else c["election_year"],
            "election_date": c["election_date"],
            "id_county": gc.norm_code(
                c["ags"] if "ags" in c.columns else c["county_code"], 5
            ),
            "id_state": gc.norm_code(c["state"], 2),
            "eligible_voters": c["eligible_voters"],
            "voters": c["number_voters"],
            "valid_votes": c["valid_votes"],
            "invalid_votes": c["invalid_votes"],
            "turnout": c["turnout"],
        }
    )
    for f in ("flag_briefwahl_agg", "flag_unsuccessful_naive_merge"):
        if f in c.columns:
            out[f] = c[f]
    return out


KEEP_COUNTY = [
    "year",
    "election_date",
    "id_county",
    "id_state",
    "eligible_voters",
    "voters",
    "valid_votes",
    "invalid_votes",
    "turnout",
    "flag_briefwahl_agg",
    "flag_unsuccessful_naive_merge",
]


def derive_wkr_long(d):
    return pd.DataFrame(
        {
            "year": d["election_year"],
            "election_date": d["election_date"],
            "id_constituency": "federal_" + d["wkr_nr"].astype(str),
            "constituency_name": d["wkr_name"],
            "id_state": gc.norm_code(d["state"], 2),
            "ballot": d["stimme"].map(gc.BALLOT).fillna(d["stimme"]),
            "eligible_voters": d["eligible_voters"],
            "voters": d["number_voters"],
            "valid_votes": d["valid_votes"],
            "invalid_votes": d["invalid_votes"],
            "turnout": d["turnout"],
        }
    )


KEEP_WKR_LONG = [
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


def derive_wkr_on25(c):
    return pd.DataFrame(
        {
            "id_constituency": "federal_" + c["wkr_nr"].astype(str),
            "constituency_name": c["wkr_name"],
            "id_state": gc.norm_code(c["state"], 2),
            "ballot": c["stimme"].map(gc.BALLOT).fillna(c["stimme"]),
            "boundary_change": c["boundary_change"],
            "eligible_voters": c["eligible_voters"],
            "valid_votes": c["valid_votes"],
        }
    )


KEEP_WKR_ON25 = [
    "year",
    "id_constituency",
    "constituency_name",
    "id_state",
    "ballot",
    "boundary_change",
    "eligible_voters",
    "valid_votes",
]


# ------------------------------------------------------------- validity utils --
def unit_key(df):
    for k in ("id_municipality", "id_county", "id_constituency"):
        if k in df.columns:
            base = [k, "year"] + (["ballot"] if "ballot" in df.columns else [])
            return base
    return ["year"]


def report(name, df, n_cells):
    key = unit_key(df)
    vs = pd.to_numeric(df["vote_share"], errors="coerce")
    nfab = int(vs.isna().sum())
    sums = df.assign(_vs=vs).groupby(key)["_vs"].sum()
    q = sums.quantile([0.01, 0.5, 0.99]).round(3).tolist()
    print(
        f"  [{name}] rows={len(df):,}  nonempty_cells={n_cells if n_cells is not None else 'n/a':}"
        f"  units={df.groupby(key).ngroups:,}  parties={df['party'].nunique()}"
    )
    print(
        f"      no-fabrication check: null vote_share rows = {nfab}"
        + (
            f"  (cells==rows: {n_cells == len(df)})"
            if n_cells is not None
            else ""
        )
    )
    print(f"      per-unit sum(vote_share) q01/med/q99 = {q}")


def official_check_2021(df):
    """Cross-check federal_constituency zweitstimme 2021 national party shares
    against published Bundestag 2021 results."""
    d = df[(df["year"] == "2021") & (df["ballot"] == "second_vote")].copy()
    d["votes"] = pd.to_numeric(d["votes"], errors="coerce")
    tot = d["votes"].sum()
    share = (d.groupby("party")["votes"].sum() / tot * 100).round(2)
    cdu_csu = share.get("cdu", 0) + share.get("csu", 0)
    print("  OFFICIAL CHECK — Bundestag 2021 Zweitstimme national shares (%):")
    print(
        f"      computed: spd={share.get('spd')}, cdu+csu={round(cdu_csu, 2)} "
        f"(official 24.1), gruene={share.get('gruene')}, fdp={share.get('fdp')}, "
        f"afd={share.get('afd')}, linke_pds={share.get('linke_pds')}"
    )
    print(
        "      official : spd=25.7, cdu+csu=24.1, gruene=14.7, fdp=11.5, afd=10.3, linke=4.9"
    )


# --------------------------------------------------------------------- main ---
def main():
    print("== FEDERAL municipality ==")
    for name, src, _harm in [
        ("federal_municipality", "federal_muni_unharm.csv", None),
        (
            "federal_municipality_harmonized_2021",
            "federal_muni_harm_21.csv",
            2021,
        ),
        (
            "federal_municipality_harmonized_2025",
            "federal_muni_harm_25.csv",
            2025,
        ),
    ]:
        df, n = gc.reshape_wide(
            os.path.join(IN, src), derive_muni, KEEP_MUNI, []
        )
        report(name, df, n)
        gc.write_parquet(df, OUT, name)

    print("== FEDERAL county ==")
    for name, src in [
        ("federal_county", "federal_cty_unharm.csv"),
        ("federal_county_harmonized_2021", "federal_cty_harm.csv"),
    ]:
        df, n = gc.reshape_wide(
            os.path.join(IN, src), derive_county, KEEP_COUNTY, []
        )
        report(name, df, n)
        gc.write_parquet(df, OUT, name)

    print("== FEDERAL constituency ==")
    df, _ = gc.passthrough_long(
        os.path.join(IN, "federal_wkr_unharm_long.csv"),
        derive_wkr_long,
        KEEP_WKR_LONG,
        ["votes", "vote_share"],
    )
    report("federal_constituency", df, None)
    official_check_2021(df)
    gc.write_parquet(df, OUT, "federal_constituency")

    df, n = gc.reshape_wide(
        os.path.join(IN, "federal_wkr_2021_on_2025.csv"),
        derive_wkr_on25,
        KEEP_WKR_ON25,
        [],
        const={"year": "2021"},
    )
    report("federal_constituency_2021_on_2025", df, n)
    gc.write_parquet(df, OUT, "federal_constituency_2021_on_2025")
    print("done.")


if __name__ == "__main__":
    main()
