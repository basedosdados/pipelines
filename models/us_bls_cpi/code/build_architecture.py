"""Generate architecture CSVs for us_bls_cpi.

One CSV per table (monthly, annual, semiannual) plus the standard `dicionario`.
Header follows the Data Basis architecture schema (see .claude/rules/data-basis-style.md).
Column descriptions are English (US dataset), first letter capitalized, no trailing period.
"""

import csv
from pathlib import Path

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

# Reusable column definitions (name -> row without the name)
YEAR = [
    "INT64",
    "Reference year of the index observation",
    "",
    "no",
    "br_bd_diretorios_data_tempo.ano:ano",
    "year",
    "no",
    "Partition column",
    "year",
]
MONTH = [
    "INT64",
    "Reference month of the index observation, from 1 to 12",
    "",
    "no",
    "br_bd_diretorios_data_tempo.mes:mes",
    "month",
    "no",
    "Derived from the BLS period code M01-M12",
    "period",
]
SURVEY = [
    "STRING",
    "CPI survey population: CPI-U for all urban consumers or CPI-W for urban wage earners and clerical workers",
    "",
    "yes",
    "",
    "",
    "no",
    "Derived from the series_id prefix (CU or CW)",
    "series_id",
]
SEASONAL = [
    "STRING",
    "Whether the series is seasonally adjusted (S) or not seasonally adjusted (U)",
    "",
    "yes",
    "",
    "",
    "no",
    "",
    "seasonal",
]
AREA = [
    "STRING",
    "BLS CPI area code identifying the geographic coverage of the series",
    "",
    "yes",
    "",
    "",
    "no",
    "Codes are CPI-specific (national, regions, size classes, metros); could map to a US geography directory in the future",
    "area_code",
]
ITEM = [
    "STRING",
    "BLS CPI item code identifying the expenditure category of the series",
    "",
    "yes",
    "",
    "",
    "no",
    "Codes follow the CPI item hierarchy (item.display_level not yet loaded)",
    "item_code",
]
BASE = [
    "STRING",
    "Index reference base period at which the index equals 100, which varies by series",
    "",
    "no",
    "",
    "",
    "no",
    "",
    "base_period",
]


def index_value(kind):
    return [
        "FLOAT64",
        f"{kind} Consumer Price Index level for the item, area, and period, relative to the base period",
        "",
        "no",
        "",
        "index",
        "no",
        "",
        "value",
    ]


def change(desc):
    return [
        "FLOAT64",
        desc,
        "",
        "no",
        "",
        "percent",
        "no",
        "Computed by Data Basis from the index series; not published by BLS",
        "value",
    ]


TABLES = {
    "monthly": [
        ("year", YEAR),
        ("month", MONTH),
        ("survey", SURVEY),
        ("seasonal_adjustment", SEASONAL),
        ("area_id", AREA),
        ("item_id", ITEM),
        ("base_period", BASE),
        ("index_value", index_value("Monthly")),
        (
            "monthly_change",
            change(
                "Percent change in the index relative to the previous month"
            ),
        ),
        (
            "twelve_month_change",
            change(
                "Percent change in the index relative to the same month of the previous year"
            ),
        ),
    ],
    "annual": [
        ("year", YEAR),
        ("survey", SURVEY),
        ("seasonal_adjustment", SEASONAL),
        ("area_id", AREA),
        ("item_id", ITEM),
        ("base_period", BASE),
        ("index_value", index_value("Annual average")),
        (
            "annual_change",
            change(
                "Percent change in the annual average index relative to the previous year"
            ),
        ),
    ],
    "semiannual": [
        ("year", YEAR),
        (
            "half",
            [
                "INT64",
                "Half of the year for semiannual series, 1 for the first half and 2 for the second half",
                "",
                "no",
                "",
                "",
                "no",
                "Derived from the BLS period code S01-S02",
                "period",
            ],
        ),
        ("survey", SURVEY),
        ("seasonal_adjustment", SEASONAL),
        ("area_id", AREA),
        ("item_id", ITEM),
        ("base_period", BASE),
        ("index_value", index_value("Semiannual")),
        (
            "semiannual_change",
            change(
                "Percent change in the index relative to the previous half-year period"
            ),
        ),
    ],
}

DICIONARIO = [
    (
        "id_tabela",
        "Slug of the us_bls_cpi table the dictionary entry describes",
    ),
    ("nome_coluna", "Name of the column the dictionary entry describes"),
    ("chave", "Coded value (key) exactly as stored in the data"),
    ("cobertura_temporal", "Temporal coverage of the key"),
    ("valor", "Human-readable label corresponding to the coded value"),
]


def main():
    out = Path(__file__).resolve().parent / "architecture"
    out.mkdir(parents=True, exist_ok=True)
    for table, cols in TABLES.items():
        with open(out / f"{table}.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(HEADER)
            for name, rest in cols:
                w.writerow([name, *rest])
    with open(out / "dicionario.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(HEADER)
        for name, desc in DICIONARIO:
            w.writerow(
                [name, "STRING", desc, "", "no", "", "", "no", "", name]
            )
    print("wrote:", ", ".join(sorted(p.name for p in out.glob("*.csv"))))


if __name__ == "__main__":
    main()
