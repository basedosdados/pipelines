#!/usr/bin/env python3
"""Emit the au_nsw_bocsar_crime architecture CSVs (one per table).

The architecture CSVs are the source of truth for column names, order, types,
English descriptions, dictionary/unit flags and directory links. clean_data.py
reads them for column order; build_columns_json.py reads them for metadata.

English dataset -> English column names (year/month/date), per
.claude/rules/data-basis-style.md.

Usage: uv run python models/au_nsw_bocsar_crime/code/build_architecture.py
"""

import csv
from pathlib import Path

ARCH = Path(__file__).resolve().parent / "architecture"
FIELDS = [
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

# Reusable column definitions: name -> (type, description_en, dir, unit, obs, original)
YEAR = (
    "year",
    "INT64",
    "Reference year of the observation",
    "no",
    "br_bd_diretorios_data_tempo.ano:ano",
    "year",
    "no",
    "Partition column",
    "",
)
MONTH = (
    "month",
    "INT64",
    "Reference month of the observation, from 1 to 12",
    "no",
    "br_bd_diretorios_data_tempo.mes:mes",
    "month",
    "no",
    "",
    "",
)
OCAT = (
    "offence_category",
    "STRING",
    "BOCSAR offence category, the top level of the offence classification",
    "no",
    "",
    "",
    "no",
    "",
    "Offence category",
)
OSUB = (
    "offence_subcategory",
    "STRING",
    "BOCSAR offence subcategory, the detailed offence type within the category",
    "no",
    "",
    "",
    "no",
    "",
    "Subcategory",
)
ABOR = (
    "aboriginality",
    "STRING",
    "Aboriginal status of the person (Aboriginal, Non-Aboriginal or Unknown)",
    "no",
    "",
    "",
    "no",
    "",
    "Aboriginality",
)
SEX = (
    "sex",
    "STRING",
    "Sex of the person (Female or Male)",
    "no",
    "",
    "",
    "no",
    "",
    "Gender",
)
SYS = (
    "custody_system",
    "STRING",
    "Custody system the record refers to (adult or youth)",
    "no",
    "",
    "",
    "no",
    "Derived from which BOCSAR custody report the row came from",
    "",
)


def col(
    name,
    btype,
    desc,
    cbd="no",
    directory="",
    unit="",
    sens="no",
    obs="",
    orig="",
):
    return {
        "name": name,
        "bigquery_type": btype,
        "description": desc,
        "temporal_coverage": "",
        "covered_by_dictionary": cbd,
        "directory_column": directory,
        "measurement_unit": unit,
        "has_sensitive_data": sens,
        "observations": obs,
        "original_name": orig,
    }


def c(tup):
    """Expand a reusable tuple into a column dict."""
    name, btype, desc, cbd, directory, unit, sens, obs, orig = tup
    return col(name, btype, desc, cbd, directory, unit, sens, obs, orig)


GEO_FK_NOTE = (
    "Identified by name only in the source; intended FK to br_bd_diretorios_au "
    "once that directory is live"
)

INC = col(
    "incidents",
    "INT64",
    "Number of criminal incidents recorded by the NSW Police Force",
    unit="incident",
    orig="",
)

TABLES = {
    # ---- Recorded Criminal Incidents (RCI) ----
    "criminal_incidents": [c(YEAR), c(MONTH), c(OCAT), c(OSUB), INC],
    "criminal_incidents_sa4": [
        c(YEAR),
        c(MONTH),
        col(
            "sa4_name",
            "STRING",
            "Name of the Statistical Area Level 4 (ASGS) in New South Wales",
            obs=GEO_FK_NOTE,
            orig="NSW Statistical Area",
        ),
        c(OCAT),
        c(OSUB),
        INC,
    ],
    "criminal_incidents_lga": [
        c(YEAR),
        c(MONTH),
        col(
            "lga_name",
            "STRING",
            "Name of the Local Government Area in New South Wales",
            obs=GEO_FK_NOTE,
            orig="LGA",
        ),
        c(OCAT),
        c(OSUB),
        INC,
    ],
    "criminal_incidents_postcode": [
        c(YEAR),
        c(MONTH),
        col(
            "postcode",
            "STRING",
            "Australian postcode of the recorded incidents",
            obs=GEO_FK_NOTE,
            orig="Postcode",
        ),
        c(OCAT),
        c(OSUB),
        INC,
    ],
    "criminal_incidents_suburb": [
        c(YEAR),
        c(MONTH),
        col(
            "suburb",
            "STRING",
            "Suburb name in New South Wales",
            obs=GEO_FK_NOTE,
            orig="Suburb",
        ),
        c(OCAT),
        c(OSUB),
        INC,
    ],
    "criminal_incidents_daily": [
        c(YEAR),
        col(
            "date",
            "DATE",
            "Calendar date of the recorded incidents",
            orig="Date",
        ),
        col(
            "offence_category",
            "STRING",
            "BOCSAR offence category mapped from the published offence label",
            obs="Mapped from the leaf offence label via the offence hierarchy",
        ),
        col(
            "offence_subcategory",
            "STRING",
            "Offence label as published in the daily file (leaf of the offence hierarchy)",
        ),
        INC,
    ],
    # ---- Alleged offenders (financial-year, annual) ----
    "alleged_offenders": [
        c(YEAR),
        col(
            "financial_year",
            "STRING",
            "Australian financial year of reference, 1 July to 30 June, e.g. 2010-11",
            orig="",
        ),
        col(
            "offence_category",
            "STRING",
            "BOCSAR offence category, the top level of the offence classification",
            orig="Offence type - main category",
        ),
        col(
            "offence_subcategory",
            "STRING",
            "BOCSAR offence subcategory, the detailed offence type within the category",
            orig="Offence type - subcategory",
        ),
        col(
            "age_group",
            "STRING",
            "Age group of the person of interest (10 to 17 years, or Adult)",
            orig="POIs Age",
        ),
        col(
            "legal_proceeding",
            "STRING",
            "Method of legal proceeding (court diversion or proceeded against to court)",
            orig="Method of legal proceeding",
        ),
        col(
            "detailed_legal_proceeding",
            "STRING",
            "Detailed method of legal proceeding within the broader method",
            orig="Detailed method of legal proceeding",
        ),
        col(
            "poi_count",
            "INT64",
            "Number of persons of interest legally proceeded against by the NSW Police Force",
            unit="person",
        ),
    ],
    # ---- Custody (adult + youth combined via custody_system) ----
    "custody_population": [
        c(YEAR),
        c(MONTH),
        c(SYS),
        col(
            "legal_status",
            "STRING",
            "Legal status of the person in custody (remand or sentenced)",
            orig="Legal Status",
        ),
        c(ABOR),
        c(SEX),
        col(
            "most_serious_offence",
            "STRING",
            "Most serious offence associated with the person in custody",
            orig="MSO",
        ),
        col(
            "people",
            "INT64",
            "Number of people in custody on the last day of the month",
            unit="person",
            orig="Count",
        ),
    ],
    "custody_receptions": [
        c(YEAR),
        c(MONTH),
        c(SYS),
        col(
            "reception_status",
            "STRING",
            "Legal status at reception into custody (remand, sentenced or unknown)",
            orig="Reception Status",
        ),
        c(ABOR),
        c(SEX),
        col(
            "receptions",
            "INT64",
            "Number of receptions into custody during the month",
            unit="reception",
            orig="Count",
        ),
    ],
    "custody_discharges": [
        c(YEAR),
        c(MONTH),
        c(SYS),
        col(
            "discharge_type",
            "STRING",
            "Type of custody the person was discharged from (remand or sentenced)",
            orig="Discharge Type",
        ),
        col(
            "discharge_type_breakdown",
            "STRING",
            "Detailed discharge destination or reason",
            orig="Discharge Type Breakdown",
        ),
        c(ABOR),
        c(SEX),
        col(
            "discharges",
            "INT64",
            "Number of discharges from custody during the month",
            unit="discharge",
            orig="Count",
        ),
    ],
    "custody_remand_to_sentenced": [
        c(YEAR),
        c(MONTH),
        c(SYS),
        col(
            "discharge_type",
            "STRING",
            "Transition recorded, remand to sentenced custody",
            orig="Discharge Type",
        ),
        c(ABOR),
        c(SEX),
        col(
            "transitions",
            "INT64",
            "Number of people who transitioned from remand to sentenced custody during the month",
            unit="transition",
            orig="Count",
        ),
    ],
}


def main():
    ARCH.mkdir(parents=True, exist_ok=True)
    for table, cols in TABLES.items():
        path = ARCH / f"{table}.csv"
        with open(path, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=FIELDS)
            w.writeheader()
            w.writerows(cols)
        print(f"{table}: {len(cols)} columns -> code/architecture/{table}.csv")


if __name__ == "__main__":
    main()
