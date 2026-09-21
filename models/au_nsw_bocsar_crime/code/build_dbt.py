#!/usr/bin/env python3
"""Generate the au_nsw_bocsar_crime dbt models (.sql) and schema.yml from the
architecture CSVs (the column source of truth) plus a per-table config.

Usage: uv run python models/au_nsw_bocsar_crime/code/build_dbt.py
"""

import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARCH = ROOT / "code" / "architecture"
DATASET = "au_nsw_bocsar_crime"

CAST = {
    "INT64": "int64",
    "FLOAT64": "float64",
    "STRING": "string",
    "DATE": "date",
}

# per-table: partition start year, cluster columns, unique key, model description
CFG = {
    "criminal_incidents": dict(
        start=1995,
        cluster=["offence_category"],
        key=["year", "month", "offence_category", "offence_subcategory"],
        desc="Monthly counts of criminal incidents recorded by the NSW Police Force for the whole state, by offence category and subcategory.",
    ),
    "criminal_incidents_sa4": dict(
        start=1995,
        cluster=["sa4_name", "offence_category"],
        key=[
            "year",
            "month",
            "sa4_name",
            "offence_category",
            "offence_subcategory",
        ],
        desc="Monthly counts of criminal incidents recorded by the NSW Police Force by Statistical Area Level 4 (ASGS), offence category and subcategory.",
    ),
    "criminal_incidents_lga": dict(
        start=1995,
        cluster=["lga_name", "offence_category"],
        key=[
            "year",
            "month",
            "lga_name",
            "offence_category",
            "offence_subcategory",
        ],
        desc="Monthly counts of criminal incidents recorded by the NSW Police Force by Local Government Area, offence category and subcategory.",
    ),
    "criminal_incidents_postcode": dict(
        start=1995,
        cluster=["postcode", "offence_category"],
        key=[
            "year",
            "month",
            "postcode",
            "offence_category",
            "offence_subcategory",
        ],
        desc="Monthly counts of criminal incidents recorded by the NSW Police Force by postcode, offence category and subcategory.",
    ),
    "criminal_incidents_suburb": dict(
        start=1995,
        cluster=["suburb", "offence_category"],
        key=[
            "year",
            "month",
            "suburb",
            "offence_category",
            "offence_subcategory",
        ],
        desc="Monthly counts of criminal incidents recorded by the NSW Police Force by suburb, offence category and subcategory.",
    ),
    "criminal_incidents_daily": dict(
        start=2010,
        cluster=["offence_category"],
        key=["date", "offence_subcategory"],
        desc="Daily counts of criminal incidents recorded by the NSW Police Force for all of New South Wales, by offence type.",
    ),
    "alleged_offenders": dict(
        start=2010,
        cluster=["offence_category"],
        key=[
            "financial_year",
            "offence_category",
            "offence_subcategory",
            "age_group",
            "legal_proceeding",
            "detailed_legal_proceeding",
        ],
        desc="Number of persons of interest legally proceeded against by the NSW Police Force per financial year, by offence type, age group and method of legal proceeding.",
    ),
    "custody_population": dict(
        start=2013,
        cluster=["custody_system", "most_serious_offence"],
        key=[
            "year",
            "month",
            "custody_system",
            "legal_status",
            "aboriginality",
            "sex",
            "most_serious_offence",
        ],
        desc="Monthly custody population (people in custody on the last day of the month) in NSW adult and youth custody, by legal status, Aboriginality, sex and most serious offence.",
    ),
    "custody_receptions": dict(
        start=2013,
        cluster=["custody_system"],
        key=[
            "year",
            "month",
            "custody_system",
            "reception_status",
            "aboriginality",
            "sex",
        ],
        desc="Monthly receptions into NSW adult and youth custody, by reception status, Aboriginality and sex.",
    ),
    "custody_discharges": dict(
        start=2013,
        cluster=["custody_system", "discharge_type"],
        key=[
            "year",
            "month",
            "custody_system",
            "discharge_type",
            "discharge_type_breakdown",
            "aboriginality",
            "sex",
        ],
        desc="Monthly discharges from NSW adult and youth custody, by discharge type and destination, Aboriginality and sex.",
    ),
    "custody_remand_to_sentenced": dict(
        start=2013,
        cluster=["custody_system"],
        key=["year", "month", "custody_system", "aboriginality", "sex"],
        desc="Monthly count of people who transitioned from remand to sentenced status within NSW adult and youth custody, by Aboriginality and sex.",
    ),
}

TIME_DIR = {
    "year": ("br_bd_diretorios_data_tempo__ano", "ano.ano"),
    "month": ("br_bd_diretorios_data_tempo__mes", "mes.mes"),
}


def read_arch(table):
    with open(ARCH / f"{table}.csv", newline="") as fh:
        return list(csv.DictReader(fh))


def sql_for(table, cfg):
    arch = read_arch(table)
    casts = ",\n".join(
        f"    safe_cast({a['name']} as {CAST[a['bigquery_type']]}) {a['name']}"
        for a in arch
    )
    cluster = ""
    if cfg["cluster"]:
        cols = ", ".join(f'"{c}"' for c in cfg["cluster"])
        cluster = f"        cluster_by=[{cols}],\n"
    return f"""{{{{
    config(
        schema="{DATASET}",
        alias="{table}",
        materialized="table",
        partition_by={{
            "field": "year",
            "data_type": "int64",
            "range": {{"start": {cfg["start"]}, "end": 2031, "interval": 1}},
        }},
{cluster}    )
}}}}


select
{casts}
from {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}} as t
"""


def yaml_desc(text, indent):
    pad = " " * indent
    return f"{pad}description: >-\n{pad}  {text}\n"


def schema_yaml():
    lines = ["---", "version: 2", "models:"]
    for table, cfg in CFG.items():
        arch = read_arch(table)
        lines.append(f"  - name: {DATASET}__{table}")
        lines.append("    description: >-")
        lines.append(f"      {cfg['desc']}")
        lines.append("    tests:")
        lines.append("      - dbt_utils.unique_combination_of_columns:")
        lines.append("          combination_of_columns:")
        for k in cfg["key"]:
            lines.append(f"            - {k}")
        lines.append("      - not_null_proportion_multiple_columns:")
        lines.append("          at_least: 0.05")
        lines.append("    columns:")
        for a in arch:
            name = a["name"]
            lines.append(f"      - name: {name}")
            lines.append(f"        description: {a['description']}")
            tests = []
            if name in ("year", "month", "date"):
                tests.append("not_null")
            col_tests = list(tests)
            rel = TIME_DIR.get(name)
            if col_tests or rel:
                lines.append("        tests:")
                for t in col_tests:
                    lines.append(f"          - {t}")
                if rel:
                    lines.append("          - relationships:")
                    lines.append(f"              to: ref('{rel[0]}')")
                    lines.append(f"              field: {rel[1]}")
    return "\n".join(lines) + "\n"


def main():
    for table, cfg in CFG.items():
        (ROOT / f"{DATASET}__{table}.sql").write_text(sql_for(table, cfg))
        print(f"wrote {DATASET}__{table}.sql")
    (ROOT / "schema.yml").write_text(schema_yaml())
    print("wrote schema.yml")


if __name__ == "__main__":
    main()
