"""Generate the us_census_lodes dbt models and schema.yml.

Both are derived from the architecture CSVs so column order, types and
descriptions cannot drift from what is registered in the backend.

    uv run python models/us_census_lodes/code/gen_dbt.py

Hand edits to the generated .sql / schema.yml are overwritten — edit this script
or the architecture instead.
"""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.us_census_lodes.constants import (
    DATASET_ID,
    YEARS,
)
from pipelines.datasets.us_census_lodes.utils import read_arch

MODEL_DIR = Path(__file__).resolve().parents[1]

PARTITIONED = {"residence_jobs", "workplace_jobs"}

# Scope the expensive tests to the latest year. The partition column here is
# `year`, so the placeholder must be the English variant: plain
# `__most_recent_year__` hardcodes `ano` in macros/custom_get_where_subquery.sql
# and fails with "Unrecognized name: ano".
MOST_RECENT_YEAR = "__most_recent_year_en__"
CLUSTER = ["state_id", "county_id"]

VINTAGE_NOTE = (
    "Os dados são enumerados em blocos censitários de 2020 e toda a série histórica "
    "foi reprocessada para essa malha; códigos de bloco de versões anteriores do LODES "
    "(malha de 2010) não são comparáveis sem os arquivos de relacionamento do Census "
    "Bureau."
)

DESCRIPTIONS = {
    "residence_jobs": (
        "Número de empregos por bloco censitário de residência do trabalhador, ano e "
        "tipo de vínculo, com desagregações por idade, faixa de rendimento, setor "
        "NAICS, raça, etnia, escolaridade e sexo. Corresponde aos arquivos Residence "
        "Area Characteristics (RAC) do LODES 8, segmento S000 (todos os trabalhadores). "
        + VINTAGE_NOTE
    ),
    "workplace_jobs": (
        "Número de empregos por bloco censitário do local de trabalho, ano e tipo de "
        "vínculo, com desagregações por idade, faixa de rendimento, setor NAICS, raça, "
        "etnia, escolaridade, sexo, idade da firma e porte da firma. Corresponde aos "
        "arquivos Workplace Area Characteristics (WAC) do LODES 8, segmento S000 "
        "(todos os trabalhadores). " + VINTAGE_NOTE
    ),
    "geography_crosswalk": (
        "Relação entre cada bloco censitário de tabulação de 2020 e as demais unidades "
        "geográficas suportadas pelo aplicativo OnTheMap, incluindo condado, setor "
        "censitário, grupo de blocos, região metropolitana, ZCTA, lugar, distrito "
        "eleitoral e distrito escolar, além do ponto interno do bloco. Retrata a "
        "delimitação vigente na divulgação do LODES 8.4 e é substituída integralmente a "
        "cada nova versão."
    ),
    "dicionario": (
        "Dicionário de códigos das colunas codificadas do conjunto us_census_lodes"
    ),
}

# Columns that are legitimately null over a large share of rows, so the
# not-null-proportion test must ignore them.
SPARSE = {
    "residence_jobs": [
        a["name"]
        for a in read_arch("residence_jobs")
        if a["name"].startswith(
            ("jobs_race_", "jobs_ethnicity_", "jobs_education_", "jobs_sex_")
        )
    ],
    "workplace_jobs": [
        a["name"]
        for a in read_arch("workplace_jobs")
        if a["name"].startswith(
            (
                "jobs_race_",
                "jobs_ethnicity_",
                "jobs_education_",
                "jobs_sex_",
                "jobs_firm_",
            )
        )
    ],
    "geography_crosswalk": [
        a["name"]
        for a in read_arch("geography_crosswalk")
        # Every geography below the county is inapplicable for some blocks
        # (tribal areas, military installations, secondary school districts...).
        if a["name"]
        not in (
            "block_id",
            "state_id",
            "state_abbreviation",
            "state_name",
            "county_id",
            "county_name",
            "census_tract_id",
            "block_group_id",
            "latitude",
            "longitude",
            "date_created",
        )
    ],
    "dicionario": [],
}


def cast(name: str, btype: str) -> str:
    if btype == "DATE":
        return f"    safe_cast({name} as date) {name},"
    return f"    safe_cast({name} as {btype.lower()}) {name},"


def build_sql(table: str) -> str:
    arch = read_arch(table)
    config = [
        f'        schema="{DATASET_ID}",',
        f'        alias="{table}",',
        '        materialized="table",',
    ]
    if table in PARTITIONED:
        config += [
            "        partition_by={",
            '            "field": "year",',
            '            "data_type": "int64",',
            f'            "range": {{"start": {YEARS[0]}, "end": {YEARS[-1] + 5}, '
            '"interval": 1},',
            "        },",
            f"        cluster_by={CLUSTER!r},",
        ]
    body = "\n".join(cast(a["name"], a["bigquery_type"]) for a in arch).rstrip(
        ","
    )
    return (
        "-- Generated by models/us_census_lodes/code/gen_dbt.py from\n"
        "-- code/architecture/<table>.csv. Edit the generator, not this file.\n"
        "{{\n    config(\n" + "\n".join(config) + "\n    )\n}}\n\n\n"
        "select\n" + body + "\nfrom\n"
        f'    {{{{ set_datalake_project("{DATASET_ID}_staging.{table}") }}}}\n'
        "    as t\n"
    )


def build_schema() -> dict:
    models = []
    for table in (
        "residence_jobs",
        "workplace_jobs",
        "geography_crosswalk",
        "dicionario",
    ):
        arch = read_arch(table)
        model: dict = {
            "name": f"{DATASET_ID}__{table}",
            "description": DESCRIPTIONS[table],
        }
        tests: list = []
        if table in PARTITIONED:
            tests.append(
                {
                    "dbt_utils.unique_combination_of_columns": {
                        "combination_of_columns": [
                            "year",
                            "job_type",
                            "block_id",
                        ],
                        "config": {"where": MOST_RECENT_YEAR},
                    }
                }
            )
        elif table == "geography_crosswalk":
            tests.append(
                {
                    "dbt_utils.unique_combination_of_columns": {
                        "combination_of_columns": ["block_id"]
                    }
                }
            )
        if table != "dicionario":
            proportion: dict = {"at_least": 0.05}
            if SPARSE[table]:
                proportion["ignore_values"] = sorted(SPARSE[table])
            if table in PARTITIONED:
                proportion["config"] = {"where": MOST_RECENT_YEAR}
            tests.append({"not_null_proportion_multiple_columns": proportion})
        if tests:
            model["tests"] = tests

        columns = []
        for a in arch:
            col: dict = {"name": a["name"], "description": a["description"]}
            col_tests: list = []
            if a["name"] in ("year", "block_id", "job_type"):
                col_tests.append("not_null")
            if a["directory_column"]:
                target, field = a["directory_column"].split(":")
                dataset, tbl = target.split(".")
                dataset = {
                    "diretorios_us": "br_bd_diretorios_us",
                    "diretorios_data_tempo": "br_bd_diretorios_data_tempo",
                }[dataset]
                # The time directory's `ano` column sits inside a STRUCT, so an
                # unqualified field binds to the struct and the test passes
                # vacuously; `ano.ano` is the working form.
                ref_field = "ano.ano" if tbl == "ano" else field
                col_tests.append(
                    {
                        "relationships": {
                            "to": f"ref('{dataset}__{tbl}')",
                            "field": ref_field,
                            "config": {"where": MOST_RECENT_YEAR}
                            if table in PARTITIONED
                            else {},
                        }
                    }
                )
            if col_tests:
                col["tests"] = col_tests
            columns.append(col)
        model["columns"] = columns
        models.append(model)
    return {"version": 2, "models": models}


def main() -> None:
    for table in (
        "residence_jobs",
        "workplace_jobs",
        "geography_crosswalk",
        "dicionario",
    ):
        path = MODEL_DIR / f"{DATASET_ID}__{table}.sql"
        path.write_text(build_sql(table), encoding="utf-8")
        print(f"wrote {path.name}")
    schema_path = MODEL_DIR / "schema.yml"
    with schema_path.open("w", encoding="utf-8") as fh:
        fh.write("---\n")
        yaml.safe_dump(
            build_schema(), fh, sort_keys=False, allow_unicode=True, width=100
        )
    print(f"wrote {schema_path.name}")


if __name__ == "__main__":
    main()
