"""Generate the us_census_bds dbt models and schema.yml from the architecture.

Usage:
    ~/.venvs/bd-pipelines/bin/python -m models.us_census_bds.code.build_dbt
"""

from __future__ import annotations

import csv
import textwrap
from pathlib import Path

DATASET = "us_census_bds"
HERE = Path(__file__).resolve().parent
ARCH = HERE / "architecture"
MODELS = HERE.parent

CLASS_COLUMNS = [
    "metro_status",
    "naics_id",
    "firm_age",
    "firm_size",
    "initial_firm_size",
    "establishment_age",
    "establishment_size",
    "initial_establishment_size",
]

# table -> (EN, PT, ES) description, shared with the metadata registration.
DESCRIPTIONS = {
    "country": (
        "Annual firm and establishment counts, employment, establishment "
        "entry and exit, job creation and destruction, and firm deaths for "
        "the United States private non-farm economy, from 1978. One row per "
        "year and cell of a breakdown: economy-wide, by metro status, by "
        "NAICS sector or vintage consistent 3- and 4-digit industry, by firm "
        "and establishment age and size, and their published crosses. Filter "
        "on breakdown before aggregating, because every breakdown tabulates "
        "the same universe. Exception: establishment_size and "
        "initial_establishment_size are exempt from the 5 percent not-null "
        "test, because only the establishment-size breakdowns populate them.",
        "Contagens anuais de firmas e estabelecimentos, emprego, entrada e "
        "saída de estabelecimentos, criação e destruição de empregos e "
        "mortes de firmas para a economia privada não agrícola dos Estados "
        "Unidos, desde 1978. Uma linha por ano e célula de um breakdown: "
        "total da economia, por situação metropolitana, por setor NAICS ou "
        "indústria de 3 e 4 dígitos consistente no tempo, por idade e "
        "tamanho de firma e de estabelecimento, e seus cruzamentos "
        "publicados. Filtre o breakdown antes de agregar, pois cada "
        "breakdown tabula o mesmo universo.",
        "Recuentos anuales de empresas y establecimientos, empleo, entrada y "
        "salida de establecimientos, creación y destrucción de empleos y "
        "muertes de empresas para la economía privada no agrícola de los "
        "Estados Unidos, desde 1978. Una fila por año y celda de un "
        "breakdown: total de la economía, por situación metropolitana, por "
        "sector NAICS o industria de 3 y 4 dígitos consistente en el tiempo, "
        "por edad y tamaño de empresa y de establecimiento, y sus cruces "
        "publicados. Filtre el breakdown antes de agregar, pues cada "
        "breakdown tabula el mismo universo.",
    ),
    "state": (
        "Annual business dynamics by state: firm and establishment counts, "
        "employment, establishment entry and exit, job creation and "
        "destruction, and firm deaths, from 1978 for the 50 states and the "
        "District of Columbia and from 1998 for Puerto Rico. One row per "
        "year, state and cell of a breakdown: by metro status, NAICS sector, "
        "firm and establishment age and size, and their published crosses. "
        "A firm with establishments in several states is counted once in "
        "each. Exception: establishment_size and initial_establishment_size "
        "are exempt from the 5 percent not-null test, because only the "
        "establishment-size breakdowns populate them.",
        "Dinâmica empresarial anual por estado: contagens de firmas e "
        "estabelecimentos, emprego, entrada e saída de estabelecimentos, "
        "criação e destruição de empregos e mortes de firmas, desde 1978 "
        "para os 50 estados e o Distrito de Colúmbia e desde 1998 para Porto "
        "Rico. Uma linha por ano, estado e célula de um breakdown: por "
        "situação metropolitana, setor NAICS, idade e tamanho de firma e de "
        "estabelecimento, e seus cruzamentos publicados. Uma firma com "
        "estabelecimentos em vários estados é contada uma vez em cada um.",
        "Dinámica empresarial anual por estado: recuentos de empresas y "
        "establecimientos, empleo, entrada y salida de establecimientos, "
        "creación y destrucción de empleos y muertes de empresas, desde 1978 "
        "para los 50 estados y el Distrito de Columbia y desde 1998 para "
        "Puerto Rico. Una fila por año, estado y celda de un breakdown: por "
        "situación metropolitana, sector NAICS, edad y tamaño de empresa y "
        "de establecimiento, y sus cruces publicados. Una empresa con "
        "establecimientos en varios estados se cuenta una vez en cada uno.",
    ),
    "county": (
        "Annual business dynamics by county: firm and establishment counts, "
        "employment, establishment entry and exit, job creation and "
        "destruction, and firm deaths, from 1978. One row per year, county "
        "and cell of a breakdown: total, NAICS sector, and coarse firm age, "
        "firm size, initial firm size and establishment age. Statewide rows "
        "hold establishments without a fixed location.",
        "Dinâmica empresarial anual por condado: contagens de firmas e "
        "estabelecimentos, emprego, entrada e saída de estabelecimentos, "
        "criação e destruição de empregos e mortes de firmas, desde 1978. "
        "Uma linha por ano, condado e célula de um breakdown: total, setor "
        "NAICS e classes grossas de idade da firma, tamanho da firma, "
        "tamanho inicial da firma e idade do estabelecimento. Linhas "
        "estaduais reúnem estabelecimentos sem localização fixa.",
        "Dinámica empresarial anual por condado: recuentos de empresas y "
        "establecimientos, empleo, entrada y salida de establecimientos, "
        "creación y destrucción de empleos y muertes de empresas, desde "
        "1978. Una fila por año, condado y celda de un breakdown: total, "
        "sector NAICS y clases gruesas de edad de la empresa, tamaño de la "
        "empresa, tamaño inicial de la empresa y edad del establecimiento. "
        "Las filas estatales reúnen establecimientos sin ubicación fija.",
    ),
    "metropolitan_area": (
        "Annual business dynamics by metropolitan and micropolitan "
        "statistical area, under the OMB delineation of July 2023: firm and "
        "establishment counts, employment, establishment entry and exit, job "
        "creation and destruction, and firm deaths, from 1978. One row per "
        "year, area and cell of a breakdown: total, NAICS sector, and coarse "
        "firm age, firm size, initial firm size and establishment age.",
        "Dinâmica empresarial anual por área estatística metropolitana e "
        "micropolitana, na delimitação do OMB de julho de 2023: contagens de "
        "firmas e estabelecimentos, emprego, entrada e saída de "
        "estabelecimentos, criação e destruição de empregos e mortes de "
        "firmas, desde 1978. Uma linha por ano, área e célula de um "
        "breakdown: total, setor NAICS e classes grossas de idade da firma, "
        "tamanho da firma, tamanho inicial da firma e idade do "
        "estabelecimento.",
        "Dinámica empresarial anual por área estadística metropolitana y "
        "micropolitana, en la delimitación de la OMB de julio de 2023: "
        "recuentos de empresas y establecimientos, empleo, entrada y salida "
        "de establecimientos, creación y destrucción de empleos y muertes "
        "de empresas, desde 1978. Una fila por año, área y celda de un "
        "breakdown: total, sector NAICS y clases gruesas de edad de la "
        "empresa, tamaño de la empresa, tamaño inicial de la empresa y edad "
        "del establecimiento.",
    ),
    "dictionary": (
        "Dictionary of the coded values used across the us_census_bds "
        "tables, one row per table, column and key.",
        "Dicionário dos valores codificados usados nas tabelas de "
        "us_census_bds, com uma linha por tabela, coluna e chave.",
        "Diccionario de los valores codificados usados en las tablas de "
        "us_census_bds, con una fila por tabla, columna y clave.",
    ),
}
TABLES = ["country", "state", "county", "metropolitan_area", "dictionary"]

# column -> (directory model, key, allowed unmatched share or None)
RELATIONSHIPS = {
    "year": ("br_bd_diretorios_data_tempo__ano", "ano.ano", None),
    "state_id": ("br_bd_diretorios_us__state", "id_state", None),
    "county_id": ("br_bd_diretorios_us__county", "id_county", None),
    "cbsa_id": ("br_bd_diretorios_us__cbsa_2023", "id_cbsa", None),
    "naics_id": ("br_bd_diretorios_us__naics_2017", "id_naics", None),
}
NOT_NULL = {"year", "breakdown", "state_id", "cbsa_id"}
# Columns excluded from the 5 percent not-null test: dimensions populated by
# a minority of breakdowns.
SPARSE = {
    "country": ["establishment_size", "initial_establishment_size"],
    "state": ["establishment_size", "initial_establishment_size"],
}


def architecture(table: str) -> list[dict[str, str]]:
    with (ARCH / f"{table}.csv").open() as f:
        return list(csv.DictReader(f))


def model_sql(table: str, columns: list[dict[str, str]]) -> str:
    """Render one dbt model that casts the all-STRING staging table."""
    names = [c["name"] for c in columns]
    config = [
        f'schema="{DATASET}"',
        f'alias="{table}"',
        'materialized="table"',
    ]
    if "year" in names:
        config.append(
            "partition_by={\n"
            '            "field": "year",\n'
            '            "data_type": "int64",\n'
            '            "range": {"start": 1978, "end": 2030, "interval": 1},\n'
            "        }"
        )
        cluster = ["breakdown"] + [
            n for n in ("state_id", "cbsa_id") if n in names
        ]
        config.append(f"cluster_by={cluster}".replace("'", '"'))
    body = ",\n".join(
        f"    safe_cast({c['name']} as {c['bigquery_type'].lower()}) {c['name']}"
        for c in columns
    )
    cfg = ",\n        ".join(config)
    return (
        "{{\n    config(\n        " + cfg + ",\n    )\n}}\n\n\n"
        f"select\n{body}\n"
        f'from {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}} as t\n'
    )


def folded(text: str, indent: int) -> str:
    """Render text as a YAML `>-` block at the given indent."""
    pad = " " * indent
    lines = textwrap.wrap(text, width=79 - indent)
    return ">-\n" + "\n".join(pad + line for line in lines)


def schema_yml() -> str:
    out = ["---", "version: 2", "models:"]
    for table in TABLES:
        columns = architecture(table)
        names = [c["name"] for c in columns]
        out.append(f"  - name: {DATASET}__{table}")
        out.append(f"    description: {folded(DESCRIPTIONS[table][0], 6)}")
        out.append("    tests:")
        if table == "dictionary":
            key = ["table_id", "column_name", "key"]
        else:
            key = [
                n
                for n in names
                if n
                in {"year", "breakdown", "state_id", "county_id", "cbsa_id"}
                or n in CLASS_COLUMNS
            ]
        out.append("      - dbt_utils.unique_combination_of_columns:")
        flow = f"          combination_of_columns: [{', '.join(key)}]"
        if len(flow) <= 80:
            # yamlfix collapses a list that fits on one line.
            out.append(flow)
        else:
            out.append("          combination_of_columns:")
            out.extend(f"            - {n}" for n in key)
        if table != "dictionary":
            out.append("      - not_null_proportion_multiple_columns:")
            out.append("          at_least: 0.05")
            if SPARSE.get(table):
                out.append(
                    f"          ignore_values: [{', '.join(SPARSE[table])}]"
                )
            dict_cols = [
                c["name"]
                for c in columns
                if c["covered_by_dictionary"] == "yes"
            ]
            out.append("      - custom_dictionary_coverage_eng:")
            out.append(
                f"          dictionary_model: ref('{DATASET}__dictionary')"
            )
            out.append(
                f"          columns_covered_by_dictionary: [{', '.join(dict_cols)}]"
            )
        out.append("    columns:")
        for c in columns:
            out.append(f"      - name: {c['name']}")
            out.append(f"        description: {folded(c['description'], 10)}")
            tests = []
            if c["name"] in NOT_NULL and table != "dictionary":
                tests.append("          - not_null")
            if c["name"] in RELATIONSHIPS and table != "dictionary":
                model, field, share = RELATIONSHIPS[c["name"]]
                if share is None:
                    tests += [
                        "          - relationships:",
                        f"              to: ref('{model}')",
                        f"              field: {field}",
                    ]
                else:
                    tests += [
                        "          - custom_relationships:",
                        f"              to: ref('{model}')",
                        f"              field: {field}",
                        f"              proportion_allowed_failures: {share}",
                    ]
            if tests == ["          - not_null"]:
                # yamlfix writes a single scalar test as a flow sequence.
                out.append("        tests: [not_null]")
            elif tests:
                out.append("        tests:")
                out.extend(tests)
    return "\n".join(out) + "\n"


def main() -> None:
    for table in TABLES:
        path = MODELS / f"{DATASET}__{table}.sql"
        path.write_text(model_sql(table, architecture(table)))
    (MODELS / "schema.yml").write_text(schema_yml())
    print(f"wrote {len(TABLES)} models and schema.yml")


if __name__ == "__main__":
    main()
