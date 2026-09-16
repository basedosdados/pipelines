"""Generate the us_bls_oes dbt models and schema.yml from the architecture CSVs.

The architecture is the single source of truth for column order, types and
descriptions (see .claude/rules/onboarding-workflow.md), so the models are
generated from it rather than hand-maintained alongside it.

Run: uv run python models/us_bls_oes/code/build_dbt.py
"""

from pathlib import Path

import yaml

from pipelines.datasets.us_bls_oes.constants import constants
from pipelines.datasets.us_bls_oes.utils import read_arch

DATASET = constants.DATASET_ID.value
MODELS = Path(__file__).resolve().parents[1]
PARTITION = {"start": 2003, "end": 2030, "interval": 1}

CLUSTER = {
    "area": ["area_type", "area_id", "occupation_id"],
    "industry": ["industry_id", "occupation_id"],
}

DESCRIPTIONS = {
    "area": (
        "Estimativas do OEWS de emprego e salários por ocupação e área "
        "geográfica, abrangendo o total nacional, estados, territórios, áreas "
        "metropolitanas e áreas não metropolitanas, para todos os setores de "
        "atividade combinados. Uma linha por ano, área, tipo de propriedade e "
        "ocupação, com emprego, salário médio e os percentis 10, 25, 50, 75 e 90 "
        "em base horária e anual. As estimativas referem-se a maio do ano "
        "indicado."
    ),
    "industry": (
        "Estimativas nacionais do OEWS de emprego e salários por ocupação e "
        "setor de atividade (NAICS). Uma linha por ano, setor, tipo de "
        "propriedade e ocupação, com emprego, participação da ocupação no "
        "emprego do setor, salário médio e os percentis 10, 25, 50, 75 e 90 em "
        "base horária e anual. As estimativas referem-se a maio do ano indicado."
    ),
    "dicionario": (
        "Dicionário de códigos das tabelas do conjunto us_bls_oes, com o rótulo "
        "correspondente a cada código armazenado nas colunas codificadas."
    ),
}

# Columns whose nulls are structural rather than a data problem: the source
# publishes the field only for some geographic levels or some years. Listed here
# so `not_null_proportion_multiple_columns` does not flag them.
SPARSE = {
    "area": [
        "state_abbreviation",
        "jobs_per_1000",
        "location_quotient",
        "hourly_wage_mean",
        "hourly_wage_percentile_10",
        "hourly_wage_percentile_25",
        "hourly_wage_median",
        "hourly_wage_percentile_75",
        "hourly_wage_percentile_90",
        "hourly_wage_only",
    ],
    "industry": [
        "industry_group",
        "percent_establishments_reporting",
        "establishments_reporting_below_threshold",
        "hourly_wage_mean",
        "hourly_wage_percentile_10",
        "hourly_wage_percentile_25",
        "hourly_wage_median",
        "hourly_wage_percentile_75",
        "hourly_wage_percentile_90",
        "hourly_wage_only",
    ],
}


def sql(table: str) -> str:
    arch = read_arch(table)
    casts = ",\n".join(
        f"    safe_cast({a['name']} as {a['bigquery_type'].lower()}) {a['name']}"
        for a in arch
    )
    cfg = [
        f'        schema="{DATASET}",',
        f'        alias="{table}",',
        '        materialized="table",',
    ]
    if table != "dicionario":
        cfg += [
            "        partition_by={",
            '            "field": "year",',
            '            "data_type": "int64",',
            f'            "range": {PARTITION},',
            "        },",
            f"        cluster_by={CLUSTER[table]},",
        ]
    return (
        "{{\n    config(\n"
        + "\n".join(cfg)
        + "\n    )\n}}\n\n\nselect\n"
        + casts
        + f'\nfrom {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}} as t\n'
    )


class Folded(str):
    """A string dbt-conventions wants emitted as a `>-` block scalar."""


def _represent_folded(dumper, data):
    return dumper.represent_scalar(
        "tag:yaml.org,2002:str", str(data), style=">"
    )


yaml.add_representer(Folded, _represent_folded, Dumper=yaml.SafeDumper)


def wrap(text: str) -> Folded:
    """Descriptions are block scalars so a colon in the text cannot break YAML."""
    return Folded(" ".join(text.split()))


def model_schema(table: str) -> dict:
    arch = read_arch(table)
    entry: dict = {
        "name": f"{DATASET}__{table}",
        "description": wrap(DESCRIPTIONS[table]),
    }
    tests: list = []
    if table != "dicionario":
        tests.append(
            {
                "dbt_utils.unique_combination_of_columns": {
                    "combination_of_columns": constants.KEYS.value[table]
                }
            }
        )
    tests.append(
        {
            "not_null_proportion_multiple_columns": {
                "at_least": 0.05,
                **(
                    {"ignore_values": SPARSE[table]} if table in SPARSE else {}
                ),
            }
        }
    )
    entry["tests"] = tests

    key = set(constants.KEYS.value.get(table, []))
    # The level-tag columns are part of the uniqueness key but are legitimately
    # null: BLS only labels the aggregation level of some rows. `industry_group`
    # did not exist before 2017; `occupation_group` (the older `group` field)
    # tags only major/total rows in the 2011-2016 releases and is blank for
    # detailed rows. Both stay in the combination key but take no not_null test.
    nullable_key = {"industry_group", "occupation_group"}
    columns = []
    for a in arch:
        c: dict = {"name": a["name"], "description": wrap(a["description"])}
        col_tests: list = []
        if a["name"] in key and a["name"] not in nullable_key:
            col_tests.append("not_null")
        if a["directory_column"]:
            ds, rest = a["directory_column"].split(".", 1)
            tbl, field = rest.split(":", 1)
            col_tests.append(
                {
                    "relationships": {
                        "to": f"ref('{ds}__{tbl}')",
                        "field": f"{tbl}.{field}",
                    }
                }
            )
        if col_tests:
            c["tests"] = col_tests
        columns.append(c)
    entry["columns"] = columns
    return entry


def main():
    for table in constants.ALL_TABLES.value:
        path = MODELS / f"{DATASET}__{table}.sql"
        path.write_text(sql(table))
        print(f"wrote {path.relative_to(MODELS.parents[1])}")

    doc = {
        "version": 2,
        "models": [model_schema(t) for t in constants.ALL_TABLES.value],
    }
    out = MODELS / "schema.yml"
    with open(out, "w") as fh:
        fh.write("---\n")
        yaml.safe_dump(doc, fh, sort_keys=False, allow_unicode=True, width=100)
    print(f"wrote {out.relative_to(MODELS.parents[1])}")


if __name__ == "__main__":
    main()
