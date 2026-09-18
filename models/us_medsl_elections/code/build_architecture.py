#!/usr/bin/env python3
"""Build architecture CSVs for us_medsl_elections (Phase 1: aggregate tables).

Emits one CSV per table under code/architecture/, following the Data Basis
10-column architecture schema. Column order in each list IS the final table
column order. Tables are normalized: geographic attributes joinable from the
US directories (state abbreviation/census/ICPSR codes, county name) are NOT
carried here — join via id_state -> br_bd_diretorios_us.state and
id_county -> br_bd_diretorios_us.county. Descriptions are in English; PT/ES
translations are added at the metadata step.
"""

import csv
import os

ARCH_COLS = [
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

FK_YEAR = "br_bd_diretorios_data_tempo.ano:ano"
FK_DATE = "br_bd_diretorios_data_tempo.data:data"
FK_STATE = "br_bd_diretorios_us.state:id_state"
FK_COUNTY = "br_bd_diretorios_us.county:id_county"

# (name, bigquery_type, description, directory_column, original_name, observations)
STATE = [
    ("year", "INT64", "Election year.", FK_YEAR, "year", ""),
    (
        "stage",
        "STRING",
        "Electoral stage: gen (general), pri (primary) or runoff.",
        "",
        "stage",
        "Null for president (general-election returns only).",
    ),
    (
        "id_state",
        "STRING",
        "State FIPS code (2-digit, zero-padded).",
        FK_STATE,
        "state_fips",
        "",
    ),
    (
        "office",
        "STRING",
        "Office contested (e.g. US PRESIDENT, US SENATE, GOVERNOR).",
        "",
        "office",
        "",
    ),
    (
        "district",
        "STRING",
        "District of the office; blank for statewide contests.",
        "",
        "district",
        "Blank/null for president and statewide senate; numbered for some 2016 state offices.",
    ),
    (
        "indicator_special",
        "BOOLEAN",
        "Whether the contest was a special election.",
        "",
        "special",
        "Null for president.",
    ),
    ("candidate", "STRING", "Candidate name, uppercase.", "", "candidate", ""),
    (
        "party_detailed",
        "STRING",
        "Full party label of the candidate's ballot line, uppercase.",
        "",
        "party_detailed",
        "For 2016 state offices, mapped from the source single-party column.",
    ),
    (
        "party_simplified",
        "STRING",
        "Party collapsed to DEMOCRAT, REPUBLICAN, LIBERTARIAN or OTHER.",
        "",
        "party_simplified",
        "Derived from party for 2016 state offices.",
    ),
    (
        "indicator_writein",
        "BOOLEAN",
        "Whether the candidate was a write-in.",
        "",
        "writein",
        "",
    ),
    (
        "mode",
        "STRING",
        "Mode of voting; TOTAL when returns are not broken down by mode.",
        "",
        "mode",
        "Null for president.",
    ),
    (
        "votes",
        "INT64",
        "Votes received by the candidate on this ballot line.",
        "",
        "candidatevotes",
        "",
    ),
    (
        "total_votes",
        "INT64",
        "Total votes cast in the contest.",
        "",
        "totalvotes",
        "",
    ),
    (
        "indicator_unofficial",
        "BOOLEAN",
        "Whether the returns are unofficial.",
        "",
        "unofficial",
        "Null for president and 2016 state offices.",
    ),
    (
        "version",
        "STRING",
        "Source dataset finalization date (YYYYMMDD).",
        "",
        "version",
        "",
    ),
]

COUNTY = [
    ("year", "INT64", "Election year.", FK_YEAR, "year", ""),
    (
        "id_state",
        "STRING",
        "State FIPS code (2-digit, zero-padded).",
        FK_STATE,
        "state_fips",
        "Resolved from the state postal abbreviation.",
    ),
    (
        "id_county",
        "STRING",
        "County FIPS code (5-digit, zero-padded).",
        FK_COUNTY,
        "county_fips",
        "Blank for non-county reporting units (e.g. overseas/UOCAVA, statewide write-in); MEDSL city codes such as 2938000 (Kansas City) are 7 digits.",
    ),
    ("office", "STRING", "Office contested (US PRESIDENT).", "", "office", ""),
    ("candidate", "STRING", "Candidate name, uppercase.", "", "candidate", ""),
    (
        "party_detailed",
        "STRING",
        "Full party label of the candidate's ballot line, uppercase.",
        "",
        "party",
        "Mapped from the source single-party column.",
    ),
    (
        "party_simplified",
        "STRING",
        "Party collapsed to DEMOCRAT, REPUBLICAN, LIBERTARIAN or OTHER.",
        "",
        "party",
        "Derived from party.",
    ),
    (
        "mode",
        "STRING",
        "Mode of voting; TOTAL when returns are not broken down by mode.",
        "",
        "mode",
        "",
    ),
    (
        "votes",
        "INT64",
        "Votes received by the candidate in this county.",
        "",
        "candidatevotes",
        "",
    ),
    (
        "total_votes",
        "INT64",
        "Total votes cast in this county contest.",
        "",
        "totalvotes",
        "",
    ),
    (
        "version",
        "STRING",
        "Source dataset finalization date (YYYYMMDD).",
        "",
        "version",
        "",
    ),
]

DISTRICT = [
    ("year", "INT64", "Election year.", FK_YEAR, "year", ""),
    (
        "stage",
        "STRING",
        "Electoral stage: gen (general), pri (primary) or runoff.",
        "",
        "stage",
        "",
    ),
    (
        "id_state",
        "STRING",
        "State FIPS code (2-digit, zero-padded).",
        FK_STATE,
        "state_fips",
        "",
    ),
    (
        "district",
        "STRING",
        "Congressional district number (2-digit, zero-padded; 00 = at-large).",
        "",
        "district",
        "",
    ),
    (
        "indicator_runoff",
        "BOOLEAN",
        "Whether the contest was a runoff election.",
        "",
        "runoff",
        "",
    ),
    (
        "indicator_special",
        "BOOLEAN",
        "Whether the contest was a special election.",
        "",
        "special",
        "",
    ),
    ("candidate", "STRING", "Candidate name, uppercase.", "", "candidate", ""),
    (
        "party_detailed",
        "STRING",
        "Full party label of the candidate's ballot line, uppercase.",
        "",
        "party",
        "Mapped from the source single-party column.",
    ),
    (
        "party_simplified",
        "STRING",
        "Party collapsed to DEMOCRAT, REPUBLICAN, LIBERTARIAN or OTHER.",
        "",
        "party",
        "Derived from party.",
    ),
    (
        "indicator_writein",
        "BOOLEAN",
        "Whether the candidate was a write-in.",
        "",
        "writein",
        "",
    ),
    (
        "mode",
        "STRING",
        "Mode of voting; TOTAL when returns are not broken down by mode.",
        "",
        "mode",
        "",
    ),
    (
        "votes",
        "INT64",
        "Votes received by the candidate on this ballot line.",
        "",
        "candidatevotes",
        "",
    ),
    (
        "total_votes",
        "INT64",
        "Total votes cast in the contest.",
        "",
        "totalvotes",
        "",
    ),
    (
        "indicator_unofficial",
        "BOOLEAN",
        "Whether the returns are unofficial.",
        "",
        "unofficial",
        "",
    ),
    (
        "indicator_fusion_ticket",
        "BOOLEAN",
        "Whether the row is a fusion/cross-endorsement ballot line.",
        "",
        "fusion_ticket",
        "",
    ),
    (
        "version",
        "STRING",
        "Source dataset finalization date (YYYYMMDD).",
        "",
        "version",
        "",
    ),
]

PRECINCT = [
    ("year", "INT64", "Election year.", FK_YEAR, "year", ""),
    ("date", "DATE", "Election date.", FK_DATE, "date", "Absent for 2016."),
    (
        "stage",
        "STRING",
        "Electoral stage: GEN, PRI or RUNOFF.",
        "",
        "stage",
        "",
    ),
    (
        "id_state",
        "STRING",
        "State FIPS code (2-digit, zero-padded).",
        FK_STATE,
        "state_fips",
        "",
    ),
    (
        "id_county",
        "STRING",
        "County FIPS code (5-digit, zero-padded); blank for non-county reporting units.",
        FK_COUNTY,
        "county_fips",
        "",
    ),
    (
        "id_jurisdiction",
        "STRING",
        "Jurisdiction FIPS code (10-digit): the reporting subdivision, equal to the county except in New England, Wisconsin and Alaska.",
        "",
        "jurisdiction_fips",
        "Absent for 2016.",
    ),
    (
        "jurisdiction_name",
        "STRING",
        "Jurisdiction name (county, or town in New England, Wisconsin and Alaska).",
        "",
        "jurisdiction_name",
        "",
    ),
    (
        "precinct",
        "STRING",
        "Precinct name as reported by the source; *FLOATING* denotes returns reported above the precinct level.",
        "",
        "precinct",
        "",
    ),
    (
        "office",
        "STRING",
        "Office contested, standardized (e.g. US PRESIDENT, US SENATE, GOVERNOR, STATE HOUSE).",
        "",
        "office",
        "",
    ),
    (
        "office_category",
        "STRING",
        "Office group of the source file: PRESIDENT, SENATE, HOUSE, STATE or LOCAL.",
        "",
        "dataverse",
        "",
    ),
    (
        "district",
        "STRING",
        "District of the office; zero-padded number, STATEWIDE, or blank.",
        "",
        "district",
        "",
    ),
    (
        "magnitude",
        "INT64",
        "Number of seats elected for the office (usually 1).",
        "",
        "magnitude",
        "Absent for 2016.",
    ),
    (
        "candidate",
        "STRING",
        "Candidate name; also OVERVOTES, UNDERVOTES, WRITE-IN.",
        "",
        "candidate",
        "",
    ),
    (
        "party_detailed",
        "STRING",
        "Full party label of the ballot line, uppercase; fusion lines joined with a slash.",
        "",
        "party_detailed",
        "",
    ),
    (
        "party_simplified",
        "STRING",
        "Party collapsed to DEMOCRAT, REPUBLICAN, LIBERTARIAN or OTHER.",
        "",
        "party_simplified",
        "",
    ),
    (
        "indicator_special",
        "BOOLEAN",
        "Whether the contest was a special election.",
        "",
        "special",
        "",
    ),
    (
        "indicator_writein",
        "BOOLEAN",
        "Whether the candidate was a write-in.",
        "",
        "writein",
        "",
    ),
    (
        "mode",
        "STRING",
        "Mode of voting; TOTAL when not broken down by mode.",
        "",
        "mode",
        "",
    ),
    (
        "votes",
        "INT64",
        "Votes received by the candidate on this ballot line in this precinct.",
        "",
        "votes",
        "",
    ),
    (
        "indicator_readme_check",
        "BOOLEAN",
        "Whether the source README notes a caveat for this row (2018-2022 only).",
        "",
        "readme_check",
        "",
    ),
]

TABLES = {
    "state": STATE,
    "county": COUNTY,
    "district": DISTRICT,
    "precinct": PRECINCT,
}


def write_table(slug, rows):
    out_dir = os.path.join(os.path.dirname(__file__), "architecture")
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{slug}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(ARCH_COLS)
        for name, bqtype, desc, fk, orig, obs in rows:
            w.writerow([name, bqtype, desc, "", "no", fk, "", "no", obs, orig])
    print(f"wrote {path} ({len(rows)} columns)")


if __name__ == "__main__":
    for slug, rows in TABLES.items():
        write_table(slug, rows)
