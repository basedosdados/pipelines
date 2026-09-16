#!/usr/bin/env python3
"""Generate architecture CSVs for au_apra_adi from the parsed workbook.

ADI columns are data-derived (measure codes generated from source labels), so
this parses the workbook to enumerate the wide-table columns in the same
first-seen order the transform uses. Long tables have a fixed 6-column schema.
English descriptions are the source labels; PT/ES translations are added by
build_columns_json.py for the fixed columns (the many wide measure columns keep
the source label as their description).

Usage:
    AU_APRA_ADI_DATA=~/Downloads/au_apra_adi_data uv run python models/au_apra_adi/code/build_architecture.py
"""

import csv
import os
from pathlib import Path

import openpyxl  # pyrefly: ignore [untyped-import]

from pipelines.datasets.au_apra_adi.constants import constants
from pipelines.datasets.au_apra_adi.utils import _iter_tabs, _unit_for

ARCH = Path(__file__).resolve().parent / "architecture"
DATA = Path(
    os.environ.get(
        "AU_APRA_ADI_DATA", Path.home() / "Downloads" / "au_apra_adi_data"
    )
)
UNIT = {
    "aud_million": "AUD million",
    "proportion": "proportion",
    "unit": "unit",
}

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


def key_rows():
    return [
        {
            "name": "year",
            "bigquery_type": "INT64",
            "description": "Reference year of the quarter-end observation",
            "temporal_coverage": "",
            "covered_by_dictionary": "no",
            "directory_column": "br_bd_diretorios_data_tempo.ano:ano",
            "measurement_unit": "year",
            "has_sensitive_data": "no",
            "observations": "Partition column",
            "original_name": "",
        },
        {
            "name": "quarter",
            "bigquery_type": "INT64",
            "description": "Reference quarter of the observation, from 1 to 4",
            "temporal_coverage": "",
            "covered_by_dictionary": "no",
            "directory_column": "",
            "measurement_unit": "quarter",
            "has_sensitive_data": "no",
            "observations": "APRA labels each quarter by its final month: Q1 March, Q2 June, Q3 September, Q4 December",
            "original_name": "",
        },
        {
            "name": "institution_type",
            "bigquery_type": "STRING",
            "description": "Authorised deposit-taking institution type or grouping",
            "temporal_coverage": "",
            "covered_by_dictionary": "yes",
            "directory_column": "",
            "measurement_unit": "",
            "has_sensitive_data": "no",
            "observations": "APRA institution groupings; some do not report every statement (e.g. foreign branch banks report no capital adequacy)",
            "original_name": "",
        },
    ]


def measure_row(code, label, unit):
    obs = (
        "Values in millions of Australian dollars"
        if unit == "aud_million"
        else (
            "Dimensionless ratio or proportion (APRA reports many of these as a fraction)"
            if unit == "proportion"
            else ""
        )
    )
    return {
        "name": code,
        "bigquery_type": "INT64" if unit == "unit" else "FLOAT64",
        "description": label,
        "temporal_coverage": "",
        "covered_by_dictionary": "no",
        "directory_column": "",
        "measurement_unit": UNIT[unit],
        "has_sensitive_data": "no",
        "observations": obs,
        "original_name": label,
    }


LONG_COLS = [
    (
        "institution_type",
        "STRING",
        "Authorised deposit-taking institution type or grouping",
        "yes",
        "",
    ),
    (
        "measure",
        "STRING",
        "Coded measure within the statement (see dictionary for the label)",
        "yes",
        "",
    ),
    (
        "unit",
        "STRING",
        "Unit of the value: aud_million, proportion or unit",
        "no",
        "",
    ),
    (
        "value",
        "FLOAT64",
        "Reported value of the measure (unit given by the unit column; NULL when not applicable or confidentiality-masked)",
        "no",
        "",
    ),
]

DIC = [
    (
        "id_tabela",
        "Slug of the au_apra_adi table the dictionary entry describes",
    ),
    ("nome_coluna", "Name of the column the dictionary entry describes"),
    ("chave", "Coded value (key) exactly as stored in the data"),
    ("cobertura_temporal", "Temporal coverage of the key"),
    ("valor", "Human-readable label corresponding to the coded value"),
]


def write_csv(name, rows):
    ARCH.mkdir(parents=True, exist_ok=True)
    with open(ARCH / f"{name}.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=HEADER)
        w.writeheader()
        w.writerows(rows)
    print(f"{name}: {len(rows)} columns")


def main():
    xlsx = next((DATA / "input").glob("*.xlsx"))
    wb = openpyxl.load_workbook(xlsx, read_only=True, data_only=True)
    # collect wide column specs (code -> (label, unit)) first-seen per subtopic
    wide = {t: {} for t in constants.WIDE_TABLES.value}
    for _tab, _it, sub, _q, rows in _iter_tabs(wb):
        if sub in wide:
            for code, label, _u, _v in rows:
                if code not in wide[sub]:
                    lbl = label.split(": ")[-1] if ": " in label else label
                    wide[sub][code] = (lbl, _unit_for(lbl, sub))
    for t in constants.WIDE_TABLES.value:
        rows = key_rows()
        for code, (label, unit) in wide[t].items():
            rows.append(measure_row(code, label, unit))
        write_csv(t, rows)
    for t in constants.LONG_TABLES.value:
        rows = key_rows()[:2]  # year, quarter
        for name, typ, desc, dic, unit in LONG_COLS:
            rows.append(
                {
                    "name": name,
                    "bigquery_type": typ,
                    "description": desc,
                    "temporal_coverage": "",
                    "covered_by_dictionary": dic,
                    "directory_column": "",
                    "measurement_unit": unit,
                    "has_sensitive_data": "no",
                    "observations": "",
                    "original_name": "",
                }
            )
        write_csv(t, rows)
    dic = [
        {
            "name": n,
            "bigquery_type": "STRING",
            "description": d,
            "temporal_coverage": "",
            "covered_by_dictionary": "no",
            "directory_column": "",
            "measurement_unit": "",
            "has_sensitive_data": "no",
            "observations": "",
            "original_name": n,
        }
        for n, d in DIC
    ]
    write_csv("dicionario", dic)


if __name__ == "__main__":
    main()
