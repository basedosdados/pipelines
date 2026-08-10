#!/usr/bin/env python3
"""Build br_bd_diretorios_de directory tables from GERDA source files.

Phase-1 geography directories (2021 boundary vintage):
  - state         : 16 Bundesländer (hardcoded canonical list)
  - county        : distinct county_code_21 from cty_crosswalks (~400 Kreise)
  - municipality  : distinct ags_21 from ags_crosswalks (~10,800 Gemeinden)

constituency and party directories are built later (they depend on the cleaned
GERDA long tables / the gerda package ParlGov lookup table).

Reads the shared GERDA crosswalk CSVs from the us_nature_gerda dataset input dir.
Writes one all-STRING snappy Parquet per table under ../output/<table>/data.parquet;
the dbt models safe_cast to final types.

Run:
    cd models/br_bd_diretorios_de/code && python3 clean.py
"""

import os
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

HERE = os.path.dirname(os.path.abspath(__file__))
GERDA_INPUT = os.path.join(HERE, "..", "..", "us_nature_gerda", "input")
LOCAL_INPUT = os.path.join(HERE, "..", "input")
OUTPUT = os.path.join(HERE, "..", "output")

sys.path.insert(0, os.path.join(HERE, "..", "..", "us_nature_gerda", "code"))
# pyrefly: ignore [missing-import]
import gerda_common as gc  # noqa: E402

# Files whose party universe feeds the party directory (everything in the GERDA
# input dir except the crosswalk sources).
NON_RESULT = {"ags_crosswalks", "cty_crosswalks"}

# GERDA far-right / far-left groupings (federal definitions, docs/state_pipeline_audit.md
# + pipeline code; union of spelling variants). Best-effort; see `left_right` from
# ParlGov for a continuous ideology score.
FAR_RIGHT = {
    "afd",
    "npd",
    "rep",
    "die_rechte",
    "dvu",
    "iii_weg",
    "fap",
    "ddd",
    "dsu",
    "die_heimat_heimat",
    "die_heimat",
    "bf_b",
    "bfb",
    "dg",
    "dns",
    "drp",
    "pro_deutschland",
    "pro_nrw",
    "the_republicans",
}
FAR_LEFT = {
    "dkp",
    "kpd",
    "mlpd",
    "sgp",
    "psg",
    "kbw",
    "spad",
    "bsa",
    "bwk",
    "v",
}
CDU_CSU = {"cdu", "csu"}
RESIDUAL = {
    "other": "residual_other",
    "waehlergruppen": "local_voter_groups",
    "einzelbewerber": "independents",
    "einzelbewerber_1": "independents",
    "einzelbewerber_2": "independents",
    "einzelbewerber_innen": "independents",
    "wgr_eb": "local_voter_groups",
}

# 16 German states: AGS 2-digit code, official abbreviation, German name, English name.
STATES = [
    ("01", "SH", "Schleswig-Holstein", "Schleswig-Holstein"),
    ("02", "HH", "Hamburg", "Hamburg"),
    ("03", "NI", "Niedersachsen", "Lower Saxony"),
    ("04", "HB", "Bremen", "Bremen"),
    ("05", "NW", "Nordrhein-Westfalen", "North Rhine-Westphalia"),
    ("06", "HE", "Hessen", "Hesse"),
    ("07", "RP", "Rheinland-Pfalz", "Rhineland-Palatinate"),
    ("08", "BW", "Baden-Württemberg", "Baden-Württemberg"),
    ("09", "BY", "Bayern", "Bavaria"),
    ("10", "SL", "Saarland", "Saarland"),
    ("11", "BE", "Berlin", "Berlin"),
    ("12", "BB", "Brandenburg", "Brandenburg"),
    ("13", "MV", "Mecklenburg-Vorpommern", "Mecklenburg-Western Pomerania"),
    ("14", "SN", "Sachsen", "Saxony"),
    ("15", "ST", "Sachsen-Anhalt", "Saxony-Anhalt"),
    ("16", "TH", "Thüringen", "Thuringia"),
]


def write_table(df, name):
    """Write df (already all-string) to output/<name>/data.parquet with an
    explicit all-STRING arrow schema."""
    out_dir = os.path.join(OUTPUT, name)
    os.makedirs(out_dir, exist_ok=True)
    schema = pa.schema([(c, pa.string()) for c in df.columns])
    table = pa.Table.from_pandas(
        df.astype(object).where(df.notna(), None),
        schema=schema,
        preserve_index=False,
    )
    pq.write_table(
        table, os.path.join(out_dir, "data.parquet"), compression="snappy"
    )
    print(f"  {name:16s} {len(df):>8,} rows  cols={list(df.columns)}")


def build_state():
    df = pd.DataFrame(
        STATES, columns=["id_state", "state_abbreviation", "name", "name_en"]
    )
    write_table(df, "state")
    return set(df["id_state"])


def build_county(state_ids):
    src = pd.read_csv(
        os.path.join(GERDA_INPUT, "cty_crosswalks.csv"),
        dtype=str,
        keep_default_na=False,
        na_values=[],
    )
    src = src[src["county_code_21"].str.len() > 0].copy()
    src["year"] = pd.to_numeric(src["year"], errors="coerce")
    # canonical name = most recent year's name per target county
    src = src.sort_values("year")
    latest = src.groupby("county_code_21", as_index=False).last()
    df = (
        pd.DataFrame(
            {
                "id_county": latest["county_code_21"],
                "id_state": latest["county_code_21"].str[:2],
                "name": latest["county_name_21"],
            }
        )
        .sort_values("id_county")
        .reset_index(drop=True)
    )
    bad = set(df["id_state"]) - state_ids
    if bad:
        print(f"  !! county rows with unknown id_state: {sorted(bad)}")
    write_table(df, "county")
    return set(df["id_county"])


def build_municipality(county_ids, state_ids):
    src = pd.read_csv(
        os.path.join(GERDA_INPUT, "ags_crosswalks.csv"),
        dtype=str,
        keep_default_na=False,
        na_values=[],
    )
    src = src[src["ags_21"].str.len() > 0].copy()
    src["year"] = pd.to_numeric(src["year"], errors="coerce")
    src = src.sort_values("year")
    latest = src.groupby("ags_21", as_index=False).last()
    df = (
        pd.DataFrame(
            {
                "id_municipality": latest["ags_21"],
                "id_county": latest["ags_21"].str[:5],
                "id_state": latest["ags_21"].str[:2],
                "name": latest["ags_name_21"],
            }
        )
        .sort_values("id_municipality")
        .reset_index(drop=True)
    )
    # integrity: municipality-derived counties/states should exist in the directories
    missing_cty = sorted(set(df["id_county"]) - county_ids)
    missing_st = sorted(set(df["id_state"]) - state_ids)
    print(
        f"  muni-derived counties not in county dir: {len(missing_cty)}"
        + (f" e.g. {missing_cty[:8]}" if missing_cty else "")
    )
    print(f"  muni-derived states not in state dir:    {missing_st}")
    write_table(df, "municipality")
    return df


def build_constituency():
    """Federal + state (Landtag) constituencies. Synthetic key matches the data
    tables: federal_<wkr_nr>, state_<state>_<wkr_nr> (wkr_nr zero-padded to 3).
    Name/state from the most recent election; the identity is year-vintage
    dependent (constituencies are redrawn), documented in the directory."""
    rows = []
    # federal
    f = pd.read_csv(
        os.path.join(GERDA_INPUT, "federal_wkr_unharm_long.csv"),
        dtype=str,
        keep_default_na=False,
        na_values=[],
        usecols=["election_year", "wkr_nr", "wkr_name", "state"],
    )
    f["election_year"] = pd.to_numeric(f["election_year"], errors="coerce")
    f = f.sort_values("election_year").drop_duplicates("wkr_nr", keep="last")
    for _, r in f.iterrows():
        rows.append(
            (
                "federal_" + str(r["wkr_nr"]).zfill(3),
                "federal",
                r["state"],
                r["wkr_name"],
            )
        )
    # state (Landtag)
    s = pd.read_csv(
        os.path.join(GERDA_INPUT, "ltw_wkr_unharm_long.csv"),
        dtype=str,
        keep_default_na=False,
        na_values=[],
        usecols=["election_year", "wkr_nr", "wkr_name", "state"],
    )
    s["election_year"] = pd.to_numeric(s["election_year"], errors="coerce")
    s = s.sort_values("election_year").drop_duplicates(
        ["state", "wkr_nr"], keep="last"
    )
    for _, r in s.iterrows():
        rows.append(
            (
                "state_" + r["state"] + "_" + str(r["wkr_nr"]).zfill(3),
                "state",
                r["state"],
                r["wkr_name"],
            )
        )
    df = pd.DataFrame(
        rows,
        columns=["id_constituency", "constituency_type", "id_state", "name"],
    )
    df = (
        df.drop_duplicates("id_constituency")
        .sort_values(["constituency_type", "id_constituency"])
        .reset_index(drop=True)
    )
    write_table(df, "constituency")


def build_party():
    """Union of all party keys observed across every GERDA result file, enriched
    with ParlGov attributes from the gerda package lookup table."""
    universe = set()
    for fn in sorted(os.listdir(GERDA_INPUT)):
        if not fn.endswith(".csv") or fn[:-4] in NON_RESULT:
            continue
        path = os.path.join(GERDA_INPUT, fn)
        cols = list(pd.read_csv(path, nrows=0).columns)
        if "party" in cols:  # already-long file
            vals = pd.read_csv(
                path,
                dtype=str,
                usecols=["party"],
                keep_default_na=False,
                na_values=[""],
            )["party"].dropna()
            universe |= set(vals.unique())
        else:  # wide file
            universe |= set(gc.party_columns(cols))
    universe -= gc.DROP_AGG
    universe -= gc.NON_PARTY
    universe = sorted(universe)
    lut = pd.read_csv(
        os.path.join(LOCAL_INPUT, "party_lookup.csv"),
        dtype=str,
        keep_default_na=False,
        na_values=[""],
    ).set_index("party_gerda")

    def look(key, col):
        return (
            lut.loc[key, col]
            if key in lut.index and col in lut.columns
            else None
        )

    rows = []
    for p in universe:
        en = (
            look(p, "party_name_english")
            or look(p, "party_name_short")
            or look(p, "party_name")
        )
        rows.append(
            {
                "id_party": p,
                "name": en if en else p.replace("_", " ").title(),
                "name_short": look(p, "party_name_short"),
                "family": look(p, "family_name"),
                "left_right": look(p, "left_right"),
                "parlgov_party_id": look(p, "party_id"),
                "is_far_right": "1" if p in FAR_RIGHT else "0",
                "is_far_left": "1" if p in FAR_LEFT else "0",
                "is_cdu_csu": "1" if p in CDU_CSU else "0",
                "category": RESIDUAL.get(p, "party"),
            }
        )
    df = pd.DataFrame(rows).sort_values("id_party").reset_index(drop=True)
    matched = df["parlgov_party_id"].notna().sum()
    print(f"  party universe: {len(df)} keys | ParlGov-matched: {matched}")
    write_table(df, "party")


def main():
    os.makedirs(OUTPUT, exist_ok=True)
    print("Building geography directories (2021 vintage) ...")
    state_ids = build_state()
    county_ids = build_county(state_ids)
    build_municipality(county_ids, state_ids)
    print("Building constituency directory ...")
    build_constituency()
    print("Building party directory ...")
    build_party()
    print("done.")


if __name__ == "__main__":
    main()
