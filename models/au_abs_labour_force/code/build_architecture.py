#!/usr/bin/env python3
"""
Emit the architecture CSVs for au_abs_labour_force (Tier 1: 4 tables).

The architecture CSV is the single source of truth for column order, BigQuery
types, dictionary/directory flags, and measurement units. English descriptions
live here; Portuguese and Spanish are attached by build_columns_json.py.

Column names are English (English-language dataset). All ABS counts are converted
from thousands to absolute persons/hours at clean time, so count columns carry the
unit `person` and hours columns `hour`; rates carry `percent`.

Categorical dimensions (sex, age_group, adjustment_type, hours_band,
status_in_employment) are decoded to readable English labels, so
covered_by_dictionary is false throughout and there is no dicionario table.

`geography` is Australia (national) plus the eight states/territories. It is left
without a directory_column for now: br_bd_diretorios_au is not yet published, and
the national aggregate does not map to a state directory row. The FK is noted in
observations and can be added in a later PR.

Usage:
    python models/au_abs_labour_force/code/build_architecture.py
"""

import csv
from pathlib import Path

OUT = Path(__file__).resolve().parent / "architecture"
OUT.mkdir(parents=True, exist_ok=True)

HEADER = [
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

# Shared dimension rows -------------------------------------------------------
YEAR = [
    "year",
    "INT64",
    "Reference year of the observation",
    "",
    "no",
    "br_bd_diretorios_data_tempo.ano:ano",
    "year",
    "no",
    "Partition column",
    "TIME_PERIOD",
]
MONTH = [
    "month",
    "INT64",
    "Reference month of the observation, from 1 to 12",
    "",
    "no",
    "br_bd_diretorios_data_tempo.mes:mes",
    "month",
    "no",
    "Derived from the monthly reference period",
    "TIME_PERIOD",
]
GEOGRAPHY = [
    "geography",
    "STRING",
    "Geographic area: Australia (national) or one of the eight states and territories",
    "",
    "no",
    "",
    "",
    "no",
    "Australia is the national aggregate; states and territories follow the ASGS. To be linked to br_bd_diretorios_au once that directory is published",
    "REGION",
]
SEX = [
    "sex",
    "STRING",
    "Sex: persons (all), males or females",
    "",
    "no",
    "",
    "",
    "no",
    "Persons is the total across males and females",
    "SEX",
]
AGE_GROUP = [
    "age_group",
    "STRING",
    "Age group in years; total covers persons aged 15 years and over",
    "",
    "no",
    "",
    "",
    "no",
    "ABS standard age groupings; the set of groups available varies by geography",
    "AGE",
]
ADJUSTMENT = [
    "adjustment_type",
    "STRING",
    "Time series adjustment: original, seasonally adjusted or trend",
    "",
    "no",
    "",
    "",
    "no",
    "",
    "TSEST",
]


def count(name, desc, original):
    return [
        name,
        "FLOAT64",
        desc,
        "",
        "no",
        "",
        "person",
        "no",
        "Converted from ABS thousands to absolute persons",
        original,
    ]


def rate(name, desc, original):
    return [name, "FLOAT64", desc, "", "no", "", "percent", "no", "", original]


def hours(name, desc, original):
    return [
        name,
        "FLOAT64",
        desc,
        "",
        "no",
        "",
        "hour",
        "no",
        "Converted from ABS thousands of hours to hours",
        original,
    ]


TABLES = {}

# 1. labour_force_status (SDMX LF) -------------------------------------------
TABLES["labour_force_status"] = [
    YEAR,
    MONTH,
    GEOGRAPHY,
    SEX,
    AGE_GROUP,
    ADJUSTMENT,
    count("employed_total", "Number of employed persons", "Employed total"),
    count(
        "employed_full_time",
        "Number of persons employed full-time",
        "Employed full-time",
    ),
    count(
        "employed_part_time",
        "Number of persons employed part-time",
        "Employed part-time",
    ),
    count(
        "unemployed_total", "Number of unemployed persons", "Unemployed total"
    ),
    count(
        "unemployed_looked_for_full_time",
        "Number of unemployed persons who looked for full-time work",
        "Unemployed looked for full-time work",
    ),
    count(
        "unemployed_looked_for_part_time",
        "Number of unemployed persons who looked for only part-time work",
        "Unemployed looked for only part-time work",
    ),
    count(
        "labour_force_total",
        "Total labour force, employed plus unemployed",
        "Labour force total",
    ),
    count(
        "not_in_labour_force",
        "Number of persons not in the labour force",
        "Not in the labour force (NILF)",
    ),
    count(
        "civilian_population_15_over",
        "Civilian population aged 15 years and over",
        "Civilian population aged 15 years and over",
    ),
    rate(
        "unemployment_rate",
        "Unemployed persons as a percentage of the labour force",
        "Unemployment rate",
    ),
    rate(
        "unemployment_rate_looked_for_full_time",
        "Unemployment rate among those who looked for full-time work",
        "Unemployment rate looked for full-time work",
    ),
    rate(
        "unemployment_rate_looked_for_part_time",
        "Unemployment rate among those who looked for only part-time work",
        "Unemployment rate looked for only part-time work",
    ),
    rate(
        "participation_rate",
        "Labour force as a percentage of the civilian population aged 15 years and over",
        "Participation rate",
    ),
    rate(
        "employment_to_population_ratio",
        "Employed persons as a percentage of the civilian population aged 15 years and over",
        "Employment to population ratio",
    ),
]

# 2. hours_worked (Table 18, national) ---------------------------------------
HOURS_BAND = [
    "hours_band",
    "STRING",
    "Range of hours actually worked in all jobs during the reference week",
    "",
    "no",
    "",
    "",
    "no",
    "Includes aggregate bands such as worked fewer than 35 hours and worked 35 hours or more",
    "Hours actually worked in all jobs",
]
TABLES["hours_worked"] = [
    YEAR,
    MONTH,
    GEOGRAPHY,
    SEX,
    HOURS_BAND,
    count(
        "employed_persons",
        "Number of employed persons in the hours-worked band",
        "Employed total",
    ),
    hours(
        "hours_worked",
        "Total hours actually worked in all jobs",
        "Number of hours actually worked in all jobs",
    ),
    hours(
        "hours_per_person",
        "Average hours actually worked per employed person",
        "Hours actually worked in all jobs per employed person",
    ),
]

# 3. status_in_employment (Table 19 national + SEM1 states) ------------------
STATUS = [
    "status_in_employment",
    "STRING",
    "Status in employment of the main job, such as employee, owner manager or contributing family worker",
    "",
    "no",
    "",
    "",
    "no",
    "",
    "Status in employment of main job",
]
TABLES["status_in_employment"] = [
    YEAR,
    MONTH,
    GEOGRAPHY,
    SEX,
    STATUS,
    count(
        "employed_total",
        "Number of employed persons with this status in employment",
        "Employed total",
    ),
    count(
        "employed_full_time",
        "Number employed full-time with this status in employment",
        "Employed full-time",
    ),
    count(
        "employed_part_time",
        "Number employed part-time with this status in employment",
        "Employed part-time",
    ),
]

# 4. underutilisation (X28 states + X29 age) ---------------------------------
TABLES["underutilisation"] = [
    YEAR,
    MONTH,
    GEOGRAPHY,
    SEX,
    AGE_GROUP,
    ADJUSTMENT,
    count(
        "underemployed_total",
        "Number of underemployed persons",
        "Underemployed total",
    ),
    rate(
        "underemployment_ratio",
        "Underemployed persons as a percentage of employed persons",
        "Underemployment ratio (proportion of employed)",
    ),
    rate(
        "underemployment_rate",
        "Underemployed persons as a percentage of the labour force",
        "Underemployment rate (proportion of labour force)",
    ),
    rate(
        "underutilisation_rate",
        "Sum of the unemployment rate and the underemployment rate",
        "Underutilisation rate",
    ),
]


def main():
    for table, rows in TABLES.items():
        path = OUT / f"{table}.csv"
        with path.open("w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(HEADER)
            w.writerows(rows)
        print(f"wrote {path}  ({len(rows)} columns)")


if __name__ == "__main__":
    main()
