"""Generate the fr_colibre_decp dbt models and schema.yml from the architecture.

Usage (from the repo root):
    python -m models.fr_colibre_decp.code.gen_dbt
    pre-commit run --files models/fr_colibre_decp/schema.yml

Descriptions are emitted as one unwrapped line each and yamlfix wraps them, so
the committed schema.yml is the formatter's output and regenerating is stable.
"""

from pathlib import Path

from pipelines.datasets.fr_colibre_decp.utils import read_architecture

DATASET = "fr_colibre_decp"
MODELS_DIR = Path(__file__).resolve().parents[1]
FIRST_YEAR, LAST_YEAR = 2014, 2031

CAST = {
    "STRING": "string",
    "DATE": "date",
    "INT64": "int64",
    "FLOAT64": "float64",
}

TABLE_DESCRIPTIONS = {
    "marche": (
        "Contratos públicos franceses sujeitos à publicação de dados essenciais "
        "(obrigatória a partir de 40 mil euros desde 2020, e de 25 mil euros antes), segundo os Dados Essenciais da Contratação Pública "
        "(DECP) consolidados por colibre.fr. Uma linha por contrato (id_marche), com "
        "os atributos do comprador e os valores da versão inicial do contrato."
    ),
    "modification": (
        "Versões dos contratos da DECP: a atribuição inicial (id_modification = 0) e "
        "cada modificação posterior, que só pode alterar o valor, a duração e os "
        "contratados. Uma linha por contrato e versão."
    ),
    "titulaire": (
        "Contratados de cada versão dos contratos da DECP. Uma linha por contrato, "
        "versão e contratado; um contrato pode ter vários contratados."
    ),
}

UNIQUE_KEYS = {
    "marche": ["id_marche"],
    "modification": ["id_marche", "id_modification"],
    "titulaire": ["id_marche", "id_modification", "id_titulaire"],
}

SPARSE = {
    "marche": ["montant_anomalie", "montant_anomalie_raisons"],
    "modification": ["montant_anomalie", "montant_anomalie_raisons"],
    "titulaire": [],
}

DIRECTORY_TESTS = {
    "code_commune_acheteur": ("commune", "id_comuna"),
    "code_departement_acheteur": ("departement", "id_departamento"),
    "code_region_acheteur": ("region", "id_regiao"),
    "code_commune_titulaire": ("commune", "id_comuna"),
    "code_departement_titulaire": ("departement", "id_departamento"),
    "code_region_titulaire": ("region", "id_regiao"),
    "code_activite_titulaire": ("naf_rev2", "naf_rev2.naf_rev2"),
}


def cast_line(name: str, bq_type: str) -> str:
    if bq_type == "GEOGRAPHY":
        return (
            f"    st_geogfromtext(safe_cast({name} as string), make_valid => true) "
            f"{name},"
        )
    return f"    safe_cast({name} as {CAST[bq_type]}) {name},"


def write_model(table: str) -> None:
    body = "\n".join(cast_line(n, t) for n, t in read_architecture(table))
    sql = f"""{{{{
    config(
        alias="{table}",
        schema="{DATASET}",
        materialized="table",
        partition_by={{
            "field": "ano",
            "data_type": "int64",
            "range": {{"start": {FIRST_YEAR}, "end": {LAST_YEAR}, "interval": 1}},
        }},
        cluster_by=["mes", "id_marche"],
    )
}}}}
select
{body.rstrip(",")}
from {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}} as t
"""
    (MODELS_DIR / f"{DATASET}__{table}.sql").write_text(sql)


def _descriptions(table: str) -> dict[str, str]:
    import csv

    from pipelines.datasets.fr_colibre_decp.utils import ARCHITECTURE_DIR

    with (ARCHITECTURE_DIR / f"{table}.csv").open(encoding="utf-8") as handle:
        return {r["name"]: r["description"] for r in csv.DictReader(handle)}


def column_tests(table: str, name: str) -> list[str]:
    if name == "ano":
        return [
            "          - not_null",
            "          - relationships:",
            "              to: ref('br_bd_diretorios_data_tempo__ano')",
            "              field: ano.ano",
        ]
    if name == "mes":
        return [
            "          - not_null",
            "          - relationships:",
            "              to: ref('br_bd_diretorios_data_tempo__mes')",
            "              field: mes.mes",
        ]
    if name == "id_marche":
        tests = ["          - not_null"]
        if table != "marche":
            tests += [
                "          - relationships:",
                f"              to: ref('{DATASET}__marche')",
                "              field: id_marche",
            ]
        return tests
    if name == "id_modification":
        return ["          - not_null"]
    if name in DIRECTORY_TESTS:
        target, field = DIRECTORY_TESTS[name]
        return [
            "          - custom_relationships:",
            f"              to: ref('br_bd_diretorios_fr__{target}')",
            f"              field: {field}",
            "              ignore_values: ['']",
            "              proportion_allowed_failures: 0.01",
        ]
    return []


def write_schema() -> None:
    lines = ["---", "version: 2", "models:"]
    for table in TABLE_DESCRIPTIONS:
        key = UNIQUE_KEYS[table]
        lines += [
            f"  - name: {DATASET}__{table}",
            "    description: >",
            f"      {TABLE_DESCRIPTIONS[table]}",
            "    tests:",
        ]
        if table == "titulaire":
            lines += [
                "      - custom_unique_combinations_of_columns:",
                f"          combination_of_columns: [{', '.join(key)}]",
                "          proportion_allowed_failures: 0.01",
            ]
        else:
            lines += [
                "      - dbt_utils.unique_combination_of_columns:",
                f"          combination_of_columns: [{', '.join(key)}]",
            ]
        lines += [
            "      - not_null_proportion_multiple_columns:",
            "          at_least: 0.05",
        ]
        if SPARSE[table]:
            lines.append("          ignore_values:")
            lines += [f"            - {c}" for c in SPARSE[table]]
        lines.append("    columns:")
        for name, description in _descriptions(table).items():
            lines += [
                f"      - name: {name}",
                "        description: >",
                f"          {description}",
            ]
            tests = column_tests(table, name)
            if tests:
                lines.append("        tests:")
                lines += tests
    (MODELS_DIR / "schema.yml").write_text("\n".join(lines) + "\n")


def main() -> None:
    for table in TABLE_DESCRIPTIONS:
        write_model(table)
        print(f"wrote {DATASET}__{table}.sql")
    write_schema()
    print("wrote schema.yml")


if __name__ == "__main__":
    main()
