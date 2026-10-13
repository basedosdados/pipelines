"""Generate the architecture CSVs for au_doe_higher_education_finances.

The architecture is the source of truth for column names, types, order and
descriptions; the dbt models and the backend metadata are both generated from
it. Writing it from one script keeps the four statement tables, which share a
schema, from drifting apart by hand.

Type follows arithmetic meaning: only genuine quantities are numeric, and every
numeric column carries a measurement unit. `hep_code` is digits but identifies a
provider, so it is STRING and links to the provider directory.
"""

from __future__ import annotations

import csv
import pathlib

OUT = pathlib.Path(__file__).resolve().parent / "architecture"

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

PROVIDER_FK = "diretorios_au.higher_education_provider:hep_code"
YEAR_FK = "br_bd_diretorios_data_tempo.ano:ano"


def column(
    name,
    bq_type,
    description,
    *,
    dictionary="no",
    directory="",
    unit="",
    observations="",
    original="",
):
    return {
        "name": name,
        "bigquery_type": bq_type,
        "description": description,
        "temporal_coverage": "",
        "covered_by_dictionary": dictionary,
        "directory_column": directory,
        "measurement_unit": unit,
        "has_sensitive_data": "no",
        "observations": observations,
        "original_name": original,
    }


def statement_columns(statement_label: str, source_table: str) -> list[dict]:
    """The four financial statement tables share one schema."""
    return [
        column(
            "year",
            "INT64",
            "Reference year of the financial statement, ending 31 December",
            directory=YEAR_FK,
            unit="year",
            observations="Partition column",
            original="Year",
        ),
        column(
            "hep_code",
            "STRING",
            "Code of the higher education provider assigned by the Department of Education",
            directory=PROVIDER_FK,
            observations=(
                "Not published in the Finance Publication, which identifies providers "
                "by name only. Resolved from the provider name against the HERDC "
                "provider list, which carries the code and back-casts every provider "
                "to its current name"
            ),
            original="(column header)",
        ),
        column(
            "institution_type",
            "STRING",
            "Sector of the provider the amount covers: whole institution, its higher "
            "education operations, or its vocational education operations",
            observations=(
                "Only dual-sector providers report a higher_education and "
                "vocational_education split, and only for the income statement; every "
                "other row is total"
            ),
            original="Institution Type",
        ),
        column(
            "line_number",
            "INT64",
            "Position of the line within the statement, in the order the source "
            "presents it",
            observations=(
                "The source repeats labels within a statement -- 'Other Comprehensive "
                "Income' heads two separate sections of the equity statement -- so the "
                "label does not identify a line and this does. It also carries the "
                "presentation order. Not stable across years: a line inserted in one "
                "release shifts every line below it, so join across years on line_item, "
                "never on this"
            ),
            original="(row position)",
        ),
        column(
            "line_item",
            "STRING",
            f"Line of the {statement_label} the amount reports",
            observations=(
                "The published label, taken verbatim from the last label column of the "
                "source sheet. The department relabels lines across the series, so a "
                "long panel on a single label can break; the line_item table records "
                "which labels appear in which years"
            ),
            original="(published row label)",
        ),
        column(
            "line_item_internal",
            "STRING",
            "Internal name of the line in the department's reporting cube, where the "
            "source publishes one",
            observations=(
                "Differs from the published label: 'DEEWR Research Grants' for "
                "'Education Research Grants', 'State Govt Total' for 'State and Local "
                "Government Financial Assistance'. Empty for 2023, the one release that "
                "ships a single label column"
            ),
            original="(cube member row label)",
        ),
        column(
            "value",
            "INT64",
            "Amount reported for the line",
            unit="AUD",
            observations=(
                "The source publishes these tables in thousands of dollars; values "
                "here are multiplied by 1,000 so they are in dollars, matching "
                "research_income. The conversion is exact"
            ),
            original=f"(cell value, {source_table})",
        ),
    ]


TABLES: dict[str, list[dict]] = {
    "income_statement": statement_columns(
        "adjusted statement of financial performance", "Table 1 / Table 2"
    ),
    "balance_sheet": statement_columns(
        "adjusted statement of financial position", "Table 3"
    ),
    "equity_movement": statement_columns(
        "adjusted statement of changes in equity and comprehensive income",
        "Table 4",
    ),
    "cash_flow": statement_columns(
        "adjusted statement of cash flows", "Table 5"
    ),
    "research_income": [
        column(
            "year",
            "INT64",
            "Reference year of the reported research income",
            directory=YEAR_FK,
            unit="year",
            observations="Partition column",
            original="Year",
        ),
        column(
            "hep_code",
            "STRING",
            "Code of the higher education provider assigned by the Department of Education",
            directory=PROVIDER_FK,
            original="HEP Code",
        ),
        column(
            "category",
            "STRING",
            "HERDC research income category, from 1 to 4",
            dictionary="yes",
            observations=(
                "1 Australian competitive grants, 2 other public sector research "
                "income, 3 industry and other research income, 4 cooperative research "
                "centre income"
            ),
            original="Category",
        ),
        column(
            "sub_category",
            "STRING",
            "HERDC research income sub-category within the category",
            observations=(
                "Sub-categories are added and retired over the series; a null amount "
                "means the sub-category was not in use that year, which is not the "
                "same as a reported nil"
            ),
            original="Sub-Category",
        ),
        column(
            "amount",
            "INT64",
            "Research income reported for the sub-category",
            unit="AUD",
            observations="Null where the sub-category was not collected that year",
            original="Amount ($)",
        ),
    ],
    "line_item": [
        column(
            "statement",
            "STRING",
            "Financial statement table the line item belongs to",
            observations="One of the four statement tables in this dataset",
        ),
        column(
            "line_item", "STRING", "Line item label as printed by the source"
        ),
        column(
            "first_year",
            "INT64",
            "First year the label appears in the series",
            unit="year",
        ),
        column(
            "last_year",
            "INT64",
            "Last year the label appears in the series",
            unit="year",
        ),
        column(
            "n_years",
            "INT64",
            "Number of years the label appears in the series",
            unit="year",
            observations=(
                "Lower than the span between first_year and last_year where a label "
                "was retired and later reinstated"
            ),
        ),
    ],
    "dicionario": [
        column(
            "id_tabela",
            "STRING",
            "Slug of the au_doe_higher_education_finances table the entry describes",
        ),
        column(
            "nome_coluna", "STRING", "Name of the column the entry describes"
        ),
        column(
            "chave",
            "STRING",
            "Coded value (key) exactly as stored in the data",
        ),
        column(
            "cobertura_temporal", "STRING", "Temporal coverage of the entry"
        ),
        column("valor", "STRING", "Meaning of the coded value"),
    ],
}


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for table, columns in TABLES.items():
        path = OUT / f"{table}.csv"
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=HEADER)
            writer.writeheader()
            writer.writerows(columns)
        print(f"{path.name}: {len(columns)} columns")


if __name__ == "__main__":
    main()
