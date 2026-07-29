#!/usr/bin/env python3
"""Generate dbt SQL models, schema.yml, and architecture CSVs for us_nature_gerda
from a single column-metadata spec, driven by the actual output/*.parquet columns.

Run: cd models/us_nature_gerda/code && python3 build_models.py [table ...]
(no args = all parquet found in ../output)
"""

import csv
import os
import sys

import pyarrow.parquet as pq

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "..", "output")
MODELS = os.path.join(HERE, "..")  # models/us_nature_gerda
ARCH = os.path.join(HERE, "architecture")
DATASET = "us_nature_gerda"

DIR_DE = "br_bd_diretorios_de"

# canonical column -> metadata
# (bq_type, description_en, measurement_unit, covered_by_dictionary, directory_column, original_name)
META = {
    "year": ("INT64", "Election year", "year", "no", "", "election_year"),
    "election_date": (
        "DATE",
        "Date of the election",
        "",
        "no",
        "",
        "election_date",
    ),
    "id_municipality": (
        "STRING",
        "Municipality identifier, 8-digit Amtlicher Gemeindeschlüssel (AGS)",
        "",
        "no",
        f"{DIR_DE}.municipality:id_municipality",
        "ags",
    ),
    "id_county": (
        "STRING",
        "County identifier, 5-digit Kreisschlüssel",
        "",
        "no",
        f"{DIR_DE}.county:id_county",
        "county",
    ),
    "id_state": (
        "STRING",
        "State identifier, 2-digit Land code",
        "",
        "no",
        f"{DIR_DE}.state:id_state",
        "state",
    ),
    "id_constituency": (
        "STRING",
        "Electoral constituency (Wahlkreis) identifier",
        "",
        "no",
        f"{DIR_DE}.constituency:id_constituency",
        "wkr_nr",
    ),
    "municipality_name": (
        "STRING",
        "Municipality name as reported by the source",
        "",
        "no",
        "",
        "ags_name",
    ),
    "constituency_name": (
        "STRING",
        "Electoral constituency (Wahlkreis) name",
        "",
        "no",
        "",
        "wkr_name",
    ),
    "ballot": (
        "STRING",
        "Ballot type: first_vote (Erststimme, candidate) or second_vote (Zweitstimme, party list)",
        "",
        "no",
        "",
        "stimme",
    ),
    "boundary_change": (
        "STRING",
        "2025 constituency boundary status relative to 2021: unchanged, redrawn, or new",
        "",
        "no",
        "",
        "boundary_change",
    ),
    "eligible_voters": (
        "INT64",
        "Number of eligible voters (Wahlberechtigte)",
        "person",
        "no",
        "",
        "eligible_voters",
    ),
    "voters": (
        "INT64",
        "Number of voters including invalid ballots (Wähler)",
        "person",
        "no",
        "",
        "number_voters",
    ),
    "valid_votes": (
        "INT64",
        "Number of valid votes (gültige Stimmen); counts votes not ballots under multi-vote systems",
        "vote",
        "no",
        "",
        "valid_votes",
    ),
    "invalid_votes": (
        "INT64",
        "Number of invalid votes (ungültige Stimmen)",
        "vote",
        "no",
        "",
        "invalid_votes",
    ),
    "turnout": (
        "FLOAT64",
        "Voter turnout as a percentage (0-100), number_voters divided by eligible_voters times 100, capped at 100",
        "percent",
        "no",
        "",
        "turnout",
    ),
    "party": (
        "STRING",
        "Party or list, GERDA normalized name",
        "",
        "no",
        f"{DIR_DE}.party:id_party",
        "party",
    ),
    "votes": ("INT64", "Votes cast for the party", "vote", "no", "", "votes"),
    "vote_share": (
        "FLOAT64",
        "Party vote share as a percentage (0-100); denominator is number_voters for federal and European, valid_votes otherwise",
        "percent",
        "no",
        "",
        "vote_share",
    ),
    "flag_naive_turnout_above_1": (
        "STRING",
        "1 if uncapped turnout exceeded 1, a mail-in allocation artifact, else 0",
        "",
        "no",
        "",
        "flag_naive_turnout_above_1",
    ),
    "flag_unsuccessful_naive_merge": (
        "STRING",
        "1 if the boundary-harmonization merge fell back to an alternative match, else 0",
        "",
        "no",
        "",
        "flag_unsuccessful_naive_merge",
    ),
    "flag_total_votes_incongruent": (
        "STRING",
        "1 if summed party votes do not match valid_votes, else 0",
        "",
        "no",
        "",
        "flag_total_votes_incongruent",
    ),
    "flag_briefwahl_agg": (
        "STRING",
        "1 for county-level mail-in aggregate rows (1994 and 1998 only), else 0",
        "",
        "no",
        "",
        "flag_briefwahl_agg",
    ),
    "flag_briefwahl_only": (
        "STRING",
        "1 if the row is a mail-in-only voting district (eligible_voters 0, valid_votes above 0), else 0",
        "",
        "no",
        "",
        "flag_briefwahl_only",
    ),
    "flag_harm_turnout_above_1": (
        "STRING",
        "1 if uncapped turnout exceeded 1 after harmonization, else 0",
        "",
        "no",
        "",
        "flag_harm_turnout_above_1",
    ),
    "flag_no_valid_votes": (
        "STRING",
        "1 if the row reports no valid votes, else 0",
        "",
        "no",
        "",
        "flag_no_valid_votes",
    ),
    "flag_other_party_residual": (
        "STRING",
        "1 if the other-party share was derived as a residual rather than reported, else 0",
        "",
        "no",
        "",
        "flag_other_party_residual",
    ),
    "flag_turnout_above_1": (
        "STRING",
        "1 if turnout was capped at 1 (European elections), else 0",
        "",
        "no",
        "",
        "flag_turnout_above_1",
    ),
    "flag_seats_total_incongruent": (
        "STRING",
        "1 if seats_total does not equal the sum of the party seat columns, else 0",
        "",
        "no",
        "",
        "flag_seats_total_incongruent",
    ),
    "election_type": (
        "STRING",
        "Type of council election",
        "",
        "no",
        "",
        "election_type",
    ),
    "county_name": (
        "STRING",
        "County name as reported by the source",
        "",
        "no",
        "",
        "county_name",
    ),
    "county_type": (
        "STRING",
        "County type: Landkreis or kreisfreie Stadt",
        "",
        "no",
        "",
        "county_type",
    ),
    "government_party": (
        "STRING",
        "Party of the county executive (Landrat or Oberbürgermeister); parteilos denotes an independent",
        "",
        "no",
        "",
        "government_party",
    ),
    "seats": (
        "INT64",
        "Council seats won by the party",
        "seat",
        "no",
        "",
        "seats",
    ),
    "seats_total": (
        "INT64",
        "Total council size (all parties)",
        "seat",
        "no",
        "",
        "seats_total",
    ),
    "seats_regional": (
        "INT64",
        "Council seats won by regional parties",
        "seat",
        "no",
        "",
        "seats_regional",
    ),
    "seats_other": (
        "INT64",
        "Council seats won by all other parties combined",
        "seat",
        "no",
        "",
        "seats_other",
    ),
    "seats_local_other": (
        "INT64",
        "Council seats held outside the six major parties (freie_wahler plus regional plus other), comparable across years",
        "seat",
        "no",
        "",
        "seats_local_other",
    ),
}

# Directory relationship tests: id_state is strict; others warn (known GERDA
# historical/SH incongruent-key and party-spelling coverage gaps).
REL_STRICT = {"id_state"}
REL_WARN = {"id_county", "id_municipality", "id_constituency", "party"}
REL_TARGET = {
    "id_state": (f"{DIR_DE}__state", "id_state"),
    "id_county": (f"{DIR_DE}__county", "id_county"),
    "id_municipality": (f"{DIR_DE}__municipality", "id_municipality"),
    "id_constituency": (f"{DIR_DE}__constituency", "id_constituency"),
    "party": (f"{DIR_DE}__party", "id_party"),
}

TABLE_DESC = {
    "federal_municipality": "Bundestag (federal) election results at the municipality level on original boundaries, long by party. Vote shares are proportions of number_voters",
    "federal_municipality_harmonized_2021": "Bundestag election results at the municipality level harmonized to 2021 boundaries, long by party",
    "federal_municipality_harmonized_2025": "Bundestag election results at the municipality level harmonized to 2025 boundaries, long by party",
    "federal_county": "Bundestag election results at the county (Kreis) level on original boundaries, long by party, from 1953",
    "federal_county_harmonized_2021": "Bundestag election results at the county level harmonized to 2021 boundaries, long by party",
    "federal_constituency": "Bundestag election results at the constituency (Wahlkreis) level, long by ballot and party, with first and second votes. Vote shares are proportions of valid_votes",
    "federal_constituency_2021_on_2025": "The 2021 Bundestag constituency result recomputed on the 2025 constituency boundaries, long by ballot and party",
    "state_municipality": "State (Landtag) election results at the municipality level on original boundaries, long by party. Vote shares are proportions of valid_votes",
    "state_municipality_harmonized_2021": "State election results at the municipality level harmonized to 2021 boundaries, long by party",
    "state_municipality_harmonized_2023": "State election results at the municipality level harmonized to 2023 boundaries, long by party",
    "state_municipality_harmonized_2025": "State election results at the municipality level harmonized to 2025 boundaries, long by party",
    "state_constituency": "State (Landtag) election results at the constituency (Wahlkreis) level, long by ballot and party, for all 16 states",
    "municipal": "Municipal council (Gemeinderat) election results on original boundaries, long by party, with council seats for the major parties. Vote shares are proportions of valid_votes",
    "municipal_harmonized_2021": "Municipal council election results harmonized to 2021 boundaries, long by party",
    "municipal_harmonized_2025": "Municipal council election results harmonized to 2025 boundaries, long by party",
    "county_council_municipality": "County council (Kreistag) election results reported at the municipality level on original boundaries, long by party",
    "county_council_municipality_harmonized_2021": "County council election results at the municipality level harmonized to 2021 boundaries, long by party",
    "county_council_county_harmonized_2021": "County council election results aggregated to the county level at 2021 boundaries, long by party",
    "county_council_seats": "County council seat composition, a county-year panel (2008-2025) long by party, with total and grouped seat counts",
    "european_municipality": "European Parliament election results at the municipality level on original boundaries, long by party. Vote shares are proportions of number_voters",
    "european_municipality_harmonized_2021": "European Parliament election results at the municipality level harmonized to 2021 boundaries, long by party",
}


def cast_expr(col):
    t = META[col][0]
    return f"safe_cast({col} as {t.lower()}) {col}"


def geo_key(cols):
    for k in ("id_municipality", "id_county", "id_constituency"):
        if k in cols:
            return k
    return None


def build_sql(table, cols):
    gk = geo_key(cols)
    cluster = [gk, "party"] if gk else ["party"]
    lines = [
        "{{",
        "    config(",
        f'        schema="{DATASET}",',
        f'        alias="{table}",',
        '        materialized="table",',
        "        partition_by={",
        '            "field": "year",',
        '            "data_type": "int64",',
        '            "range": {"start": 1945, "end": 2031, "interval": 1},',
        "        },",
        f"        cluster_by={cluster},",
        "    )",
        "}}",
        "",
        "select",
    ]
    casts = [f"    {cast_expr(c)}" for c in cols]
    lines.append(",\n".join(casts))
    lines.append(
        f'from {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}} as t'
    )
    with open(os.path.join(MODELS, f"{DATASET}__{table}.sql"), "w") as f:
        f.write("\n".join(lines) + "\n")


SPARSE = {
    "seats",
    "election_type",
    "election_date",
    "government_party",
    "county_type",
    "county_name",
    "boundary_change",
    "invalid_votes",
    "seats_total",
    "seats_regional",
    "seats_other",
    "seats_local_other",
}


def schema_block(table, cols):
    gk = geo_key(cols)
    # election_date distinguishes multiple elections in one calendar year
    # (e.g. Hamburg held two state elections in 1982)
    keyparts = (
        ["year"]
        + ([gk] if gk else [])
        + (["election_date"] if "election_date" in cols else [])
        + (["ballot"] if "ballot" in cols else [])
        + ["party"]
    )
    notnull = set(
        ["year", "party"]
        + ([gk] if gk else [])
        + (["ballot"] if "ballot" in cols else [])
    )
    ignore = [c for c in cols if c.startswith("flag_") or c in SPARSE]
    prop = [
        "      - not_null_proportion_multiple_columns:",
        "          at_least: 0.05",
    ]
    if ignore:
        prop.append("          ignore_values:")
        prop += [f"            - {c}" for c in ignore]
    out = [
        f"  - name: {DATASET}__{table}",
        "    description: >",
        f"      {TABLE_DESC[table]}",
        "    tests:",
        "      - dbt_utils.unique_combination_of_columns:",
        f"          combination_of_columns: {keyparts}",
        *prop,
        "    columns:",
    ]
    for c in cols:
        _, desc = META[c][0], META[c][1]
        out.append(f"      - name: {c}")
        out.append("        description: >")
        out.append(f"          {desc}")
        tests = []
        if c in notnull:
            tests.append("          - not_null")
        if c in REL_TARGET:
            tgt, fld = REL_TARGET[c]
            sev = "error" if c in REL_STRICT else "warn"
            tests.append("          - relationships:")
            tests.append(f"              to: ref('{tgt}')")
            tests.append(f"              field: {fld}")
            tests.append(
                f"              config:\n                severity: {sev}"
            )
        if tests:
            out.append("        tests:")
            out.extend(tests)
    return "\n".join(out)


def build_arch(table, cols):
    os.makedirs(ARCH, exist_ok=True)
    hdr = [
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
    with open(os.path.join(ARCH, f"{table}.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(hdr)
        for c in cols:
            bt, desc, unit, dic, direc, orig = META[c]
            w.writerow([c, bt, desc, "", dic, direc, unit, "no", "", orig])


def main():
    tables = sys.argv[1:] or sorted(
        f[:-8] for f in os.listdir(OUT) if f.endswith(".parquet")
    )
    blocks = []
    for t in tables:
        cols = list(pq.read_schema(os.path.join(OUT, f"{t}.parquet")).names)
        build_sql(t, cols)
        build_arch(t, cols)
        blocks.append(schema_block(t, cols))
        print(f"  built {t}: {len(cols)} cols -> sql, arch csv")
    schema = "---\nversion: 2\n\nmodels:\n" + "\n".join(blocks) + "\n"
    with open(os.path.join(MODELS, "schema.yml"), "w") as f:
        f.write(schema)
    print(f"wrote schema.yml ({len(tables)} models)")


if __name__ == "__main__":
    main()
