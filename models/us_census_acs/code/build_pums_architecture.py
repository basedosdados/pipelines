#!/usr/bin/env python3
"""Build draft person/household architecture CSVs for us_census_acs PUMS (hybrid schema).

Hybrid rule (user-approved):
  - canonical column names = case-normalized (lowercase in output, mnemonic preserved)
  - fold high-confidence pure renames into canonical (RENAME map)
  - keep genuinely-distinct vintage code variables as separate columns
Types from dict C/N flag: C->STRING, N->INT64 (all PUMS numerics are integer-stored).
Overrides: sequence/id N-vars -> STRING; ADJINC/ADJHSG stay STRING (implied-decimal factors).
Descriptions: EN from dict now; PT/ES filled in a later pass.
"""

import csv
import json
import os

WORK = "/Users/rdahis/acs_data/work"
OUT = "/Users/rdahis/Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/pipelines/.claude/worktrees/medsl-election-data-onboard-7f88f7/models/us_census_acs/code/architecture"
os.makedirs(OUT, exist_ok=True)
with open(f"{WORK}/pums_vardict.json") as f:
    vardict = json.load(f)
with open(f"{WORK}/pums_person_cols.txt") as f:
    person_cols = f.read().split("\n")
with open(f"{WORK}/pums_housing_cols.txt") as f:
    housing_cols = f.read().split("\n")

# --- high-confidence pure renames (old -> canonical); folded (dropped in favor of canonical) ---
RENAME = {"ST": "STATE", "BDS": "BDSP", "RMS": "RMSP", "VAL": "VALP"}

# --- N-vars that are really identifiers/sequences -> STRING (no arithmetic meaning) ---
FORCE_STRING = {"SPORDER"}

# --- measurement units for genuine numeric quantities ---
USD = "USD"
UNIT = {
    # person
    "AGEP": "years",
    "CITWP": "year",
    "INTP": USD,
    "JWMNP": "minutes",
    "MARHYP": "year",
    "OIP": USD,
    "PAP": USD,
    "RETP": USD,
    "SEMP": USD,
    "SSIP": USD,
    "SSP": USD,
    "WAGP": USD,
    "WKHP": "hours",
    "WKWN": "weeks",
    "YOEP": "year",
    "PERNP": USD,
    "PINCP": USD,
    "POVPIP": "percent",
    "JWRIP": "percent",
    # housing
    "NP": "persons",
    "BDSP": "bedrooms",
    "CONP": USD,
    "ELEP": USD,
    "FULP": USD,
    "GASP": USD,
    "INSP": USD,
    "MHP": USD,
    "MRGP": USD,
    "RMSP": "rooms",
    "RNTP": USD,
    "SMP": USD,
    "VALP": USD,
    "WATP": USD,
    "FINCP": USD,
    "GRNTP": USD,
    "GRPIP": "percent",
    "HHLDRAGEP": "years",
    "HINCP": USD,
    "NOC": "persons",
    "NPF": "persons",
    "NRC": "persons",
    "OCPIP": "percent",
    "SMOCP": USD,
    "TAXAMT": USD,
    "MV": "years",
    "YBL": "year",
}


def is_weight(v):
    return (
        v == "PWGTP"
        or v == "WGTP"
        or v.startswith("PWGTP")
        or v.startswith("WGTP")
    )


def geo_fk(v):
    return "br_bd_diretorios_us.state:id_state" if v == "STATE" else ""


def partition_rows():
    return [
        {
            "name": "year",
            "bigquery_type": "INT64",
            "description": "Census reference year; for 5-year estimates this is the final year of the 5-year period",
            "temporal_coverage": "",
            "covered_by_dictionary": "no",
            "directory_column": "br_bd_diretorios_data_tempo.ano:ano",
            "measurement_unit": "",
            "has_sensitive_data": "no",
            "observations": "Partition column (hive path 'ano=' in staging).",
            "original_name": "ano",
        },
        {
            "name": "period",
            "bigquery_type": "STRING",
            "description": "ACS estimate period: 1-year or 5-year",
            "temporal_coverage": "",
            "covered_by_dictionary": "no",
            "directory_column": "",
            "measurement_unit": "",
            "has_sensitive_data": "no",
            "observations": "Values: 1-year, 5-year. Part of the logical key.",
            "original_name": "",
        },
    ]


def build(cols, kind):
    rows = list(partition_rows())
    folded, kept_hist, review_unit = [], [], []
    for v in cols:
        if not v:
            continue
        if v in RENAME:  # fold pure rename into canonical
            folded.append(f"{v}->{RENAME[v]}")
            continue
        meta = vardict.get(v)
        in_dict = meta is not None
        typ = (
            meta["type"] if in_dict else "C"
        )  # historical-only assumed categorical code
        desc = (
            meta["desc"]
            if in_dict
            else f"[VINTAGE/HISTORICAL variable {v} — define from legacy dict]"
        )
        has_labels = meta["has_labels"] if in_dict else True
        if not in_dict:
            kept_hist.append(v)

        # type
        bqtype = "STRING" if v in FORCE_STRING or typ == "C" else "INT64"

        # unit / dict / directory
        unit = ""
        covered = "no"
        directory = geo_fk(v)
        obs = ""
        if bqtype == "INT64":
            if is_weight(v):
                unit = ""
                obs = "Dimensionless statistical replicate weight (no measurement unit)."
            elif v in UNIT:
                unit = UNIT[v]
            else:
                unit = "REVIEW"
                review_unit.append(v)
        else:  # STRING
            if v in ("SERIALNO",):
                covered = "no"
                obs = "Housing-unit/person serial identifier."
            elif v in ("STATE",):
                covered = "no"  # directory
            elif v in (
                "PUMA",
                "PUMA00",
                "POWPUMA",
                "MIGPUMA",
                "POWPUMA00",
                "MIGPUMA00",
            ):
                covered = "no"
                obs = "Geographic code; PUMA vintage varies by year (D6) — no directory FK in v1."
            elif v in ("ADJINC", "ADJHSG", "ADJUST"):
                covered = "no"
                obs = "Adjustment factor with 6 implied decimal places (divide by 1e6)."
            elif has_labels:
                covered = "yes"
            else:
                covered = "no"

        rows.append(
            {
                "name": v.lower(),
                "bigquery_type": bqtype,
                "description": desc,
                "temporal_coverage": "",
                "covered_by_dictionary": covered,
                "directory_column": directory,
                "measurement_unit": unit,
                "has_sensitive_data": "no",
                "observations": obs,
                "original_name": v,
            }
        )

    # write CSV
    fields = [
        "name",
        "bigquery_type",
        "description",
        "temporal_coverage",
        "covered_by_dictionary",
        "directory_column",
        "measurement_unit",
        "has_sensitive_data",
        "observations",
        "original_name",
    ]
    with open(f"{OUT}/{kind}.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)
    return rows, folded, kept_hist, review_unit


report = []
for cols, kind in [
    (person_cols, "microdata_person"),
    (housing_cols, "microdata_household"),
]:
    rows, folded, kept_hist, review_unit = build(cols, kind)
    nstr = sum(1 for r in rows if r["bigquery_type"] == "STRING")
    nint = sum(1 for r in rows if r["bigquery_type"] == "INT64")
    nwt = sum(1 for r in rows if is_weight(r["original_name"]))
    ndict = sum(1 for r in rows if r["covered_by_dictionary"] == "yes")
    report.append(f"\n===== {kind}: {len(rows)} columns =====")
    report.append(
        f"  STRING={nstr} (dict-covered={ndict}), INT64={nint} (of which {nwt} replicate weights)"
    )
    report.append(f"  folded pure-renames ({len(folded)}): {folded}")
    report.append(
        f"  kept historical/vintage cols not in modern dict ({len(kept_hist)}): {kept_hist}"
    )
    report.append(
        f"  numeric cols needing UNIT review ({len(review_unit)}): {review_unit}"
    )

with open(f"{OUT}/../harmonization_report.txt", "w") as f:
    f.write("\n".join(report))
print("\n".join(report))
print(f"\nwrote {OUT}/microdata_person.csv, microdata_household.csv")
