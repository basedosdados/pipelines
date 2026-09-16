"""Generate the dbt models and schema.yml from the architecture CSVs.

Column order and types come from the architecture, which is the source of truth,
so the models cannot drift from it. Everything in staging is STRING; each model
safe_casts to the architecture's declared type.
"""

from __future__ import annotations

import csv
import pathlib

CODE = pathlib.Path(__file__).resolve().parent
ARCH = CODE / "architecture"
MODELS = CODE.parent.parent / "au_doe_higher_education_finances"

DATASET = "au_doe_higher_education_finances"

# Partition range per table. The statements start in the earliest year recovered
# from the archive; research income starts with HERDC itself in 1992.
PARTITIONS = {
    "income_statement": (2008, 2031),
    "balance_sheet": (2008, 2031),
    "equity_movement": (2008, 2031),
    "cash_flow": (2008, 2031),
    "research_income": (1992, 2031),
}

PRIMARY_KEYS = {
    "income_statement": [
        "year",
        "hep_code",
        "institution_type",
        "line_number",
    ],
    "balance_sheet": ["year", "hep_code", "institution_type", "line_number"],
    "equity_movement": ["year", "hep_code", "institution_type", "line_number"],
    "cash_flow": ["year", "hep_code", "institution_type", "line_number"],
    "research_income": ["year", "hep_code", "category", "sub_category"],
    "line_item": ["statement", "line_item"],
    "dicionario": ["id_tabela", "nome_coluna", "chave"],
}

NOT_NULL = {
    "line_number",
    "year",
    "hep_code",
    "institution_type",
    "line_item",
    "statement",
    "category",
    "sub_category",
}

DESCRIPTIONS = {
    "income_statement": (
        "Adjusted statement of financial performance of Australian higher education "
        "providers, one row per provider, year, sector and revenue or expense line. "
        "Sourced from the Department of Education Finance Publication."
    ),
    "balance_sheet": (
        "Adjusted statement of financial position of Australian higher education "
        "providers, one row per provider, year and asset, liability or equity line. "
        "Sourced from the Department of Education Finance Publication."
    ),
    "equity_movement": (
        "Adjusted statement of changes in equity and comprehensive income of Australian "
        "higher education providers, one row per provider, year and movement line. "
        "Sourced from the Department of Education Finance Publication."
    ),
    "cash_flow": (
        "Adjusted statement of cash flows of Australian higher education providers, one "
        "row per provider, year and cash flow line. Sourced from the Department of "
        "Education Finance Publication."
    ),
    "research_income": (
        "Research and development income of Australian higher education providers, one "
        "row per provider, year, category and sub-category. Collected through the Higher "
        "Education Research Data Collection (HERDC) and used to allocate research block "
        "grants. A null amount means the sub-category was not in use that year."
    ),
    "line_item": (
        "Which line item labels appear in each financial statement and over which years. "
        "The department relabels items across the series, so this is the map needed to "
        "read a long panel on any single line."
    ),
    "dicionario": (
        "Dictionary of the coded values used across the au_doe_higher_education_finances "
        "tables."
    ),
}


def read_architecture(table: str) -> list[dict]:
    with (ARCH / f"{table}.csv").open(encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def sql_for(table: str, columns: list[dict]) -> str:
    config = [
        f'        schema="{DATASET}"',
        f'        alias="{table}"',
        '        materialized="table"',
    ]
    if table in PARTITIONS:
        start, end = PARTITIONS[table]
        config.append(
            "        partition_by={\n"
            '            "field": "year",\n'
            '            "data_type": "int64",\n'
            f'            "range": {{"start": {start}, "end": {end}, "interval": 1}},\n'
            "        }"
        )
    if table in (
        "income_statement",
        "balance_sheet",
        "equity_movement",
        "cash_flow",
    ):
        config.append('        cluster_by=["hep_code", "line_item"]')
    elif table == "research_income":
        config.append('        cluster_by=["hep_code", "category"]')

    selects = ",\n".join(
        f"    safe_cast({c['name']} as {c['bigquery_type'].lower()}) {c['name']}"
        for c in columns
    )
    return (
        "{{\n    config(\n" + ",\n".join(config) + ",\n    )\n}}\n\n\n"
        f"select\n{selects}\n"
        f'from {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}} as t\n'
    )


def wrap(text: str, indent: str, width: int = 84) -> list[str]:
    words, lines, current = text.split(), [], indent
    for word in words:
        if len(current) + len(word) + 1 > width and current.strip():
            lines.append(current.rstrip())
            current = indent + word
        else:
            current = f"{current} {word}" if current.strip() else indent + word
    if current.strip():
        lines.append(current.rstrip())
    return lines


def schema_yaml() -> str:
    out = ["---", "version: 2", "models:"]
    for table in PRIMARY_KEYS:
        columns = read_architecture(table)
        out.append(f"  - name: {DATASET}__{table}")
        out.append("    description: >-")
        out += wrap(DESCRIPTIONS[table], "      ")
        out.append("    tests:")
        out.append("      - dbt_utils.unique_combination_of_columns:")
        out.append("          combination_of_columns:")
        for key in PRIMARY_KEYS[table]:
            out.append(f"            - {key}")
        out.append("      - not_null_proportion_multiple_columns:")
        out.append("          at_least: 0.05")
        out.append("    columns:")
        for col in columns:
            name = col["name"]
            out.append(f"      - name: {name}")
            out.append("        description: >-")
            out += wrap(col["description"], "          ")
            tests: list[str] = []
            if name in NOT_NULL:
                tests.append("          - not_null")
            if name == "year":
                tests += [
                    "          - relationships:",
                    "              to: ref('br_bd_diretorios_data_tempo__ano')",
                    "              field: ano.ano",
                ]
            if name == "hep_code":
                tests += [
                    "          - relationships:",
                    "              to: ref('br_bd_diretorios_au__higher_education_provider')",
                    "              field: hep_code",
                ]
            if tests:
                out.append("        tests:")
                out += tests
    return "\n".join(out) + "\n"


def main() -> None:
    MODELS.mkdir(parents=True, exist_ok=True)
    for table in PRIMARY_KEYS:
        columns = read_architecture(table)
        path = MODELS / f"{DATASET}__{table}.sql"
        path.write_text(sql_for(table, columns))
        print(f"{path.name}: {len(columns)} columns")
    (MODELS / "schema.yml").write_text(schema_yaml())
    print("schema.yml written")


if __name__ == "__main__":
    main()
