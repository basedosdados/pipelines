"""The full (table -> ordered column list) layout of the dataset.

One place that knows every table and every column, so the architecture
generator, the cleaning code, the dbt models and the metadata step all agree.
"""

import json
from pathlib import Path

import constants as c
import naming
import tables

with open(Path(__file__).resolve().parent / "headers.json") as _fh:
    HEADERS = json.load(_fh)

# Summary tables whose source file already carries Program_Year. The rest get
# the program year injected from the file name.
SUMMARY_WITH_YEAR = {
    "summary_state_by_nature",
    "summary_national",
    "summary_national_by_specialty",
    "summary_state",
    "summary_teaching_hospital",
    "summary_reporting_entity",
    "summary_physician",
}

# The dashboard is published as a pivot: one column per program year. Stored
# long instead, so adding a program year adds rows rather than columns.
DASHBOARD_COLUMNS = ["year", "dashboard_row_number", "metric", "value"]

# The dictionary table keeps its Portuguese column names in every dataset,
# English ones included: it is a Data Basis structure, not dataset content.
DICIONARIO_COLUMNS = [
    "id_tabela",
    "nome_coluna",
    "chave",
    "cobertura_temporal",
    "valor",
]


def _summary_columns(name: str) -> list[str]:
    if name == "summary_dashboard":
        return list(DASHBOARD_COLUMNS)
    cols = [
        naming.rename_summary(s)
        for s in HEADERS["summary"][name]
        if not s.startswith("PY_") and s != "Total"
    ]
    if name not in SUMMARY_WITH_YEAR:
        cols = ["year", *cols]
    return tables.order(cols)


def build() -> dict[str, list[str]]:
    layout: dict[str, list[str]] = {}
    layout.update(tables.all_tables())
    for name, cols in HEADERS["profile"].items():
        layout[name] = [naming.rename_profile(name, s) for s in cols]
    for name in list(c.SUMMARY_PER_YEAR) + list(c.SUMMARY_ALL_YEARS):
        layout[name] = _summary_columns(name)
    layout["dicionario"] = list(DICIONARIO_COLUMNS)
    return layout


LAYOUT = build()

# Program-year coverage per table, used for partition ranges and metadata.
COVERAGE = {
    "general": c.MODERN_YEARS,
    "general_legacy": c.LEGACY_YEARS,
    "research": c.MODERN_YEARS,
    "research_legacy": c.LEGACY_YEARS,
    "research_principal_investigator": c.ALL_YEARS,
    "ownership": c.ALL_YEARS,
    **{
        name: c.SUMMARY_YEARS
        for name in list(c.SUMMARY_PER_YEAR) + list(c.SUMMARY_ALL_YEARS)
    },
}

# Entity tables are a single snapshot of the current publication, not a
# per-year series, so they carry no year column and no partition.
UNPARTITIONED = set(HEADERS["profile"]) | {"dicionario"}


if __name__ == "__main__":
    for name, cols in LAYOUT.items():
        years = COVERAGE.get(name)
        span = f"{years[0]}-{years[-1]}" if years else "snapshot"
        print(f"{name:38s} {len(cols):3d} cols  {span}")
    print(
        f"\n{len(LAYOUT)} tables, {sum(len(v) for v in LAYOUT.values())} columns"
    )
