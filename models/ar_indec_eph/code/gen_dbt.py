"""Generate the dbt models and schema.yml for ar_indec_eph from the architecture.

The architecture CSVs are the source of truth for column order, names and types,
so the SQL is generated rather than hand-maintained: 348 safe_cast lines across
two models are not worth editing by hand, and drift between the CSV and the SQL
is the failure mode this avoids.

Run after build_architecture.py, and after null_proportions.py if the sparse
column list needs refreshing.
"""

import csv
import json

from models.ar_indec_eph.code.constants import ARCH_DIR, CODE_DIR, TABLES

DATASET = "ar_indec_eph"
MODEL_DIR = CODE_DIR.parent
PARTITION_START, PARTITION_END = 2003, 2031

# Directories that keep their conceptual foreign key in the architecture but get
# no dbt relationships test. Empty: both time directories this dataset links to
# resolve in dev and in prod.
#
# Note on br_bd_diretorios_data_tempo__trimestre: that model calls a macro that
# does not exist (set_dalake_project, missing the "ta"), so it cannot be rebuilt.
# Its table nevertheless exists and is correct in both projects -- 4 rows, 1 to 4
# -- so the relationships test below resolves and is meaningful. dbt parse is
# unaffected, because an undefined macro fails at compile time for that model
# alone. The typo is still worth fixing so the directory can be refreshed.
DIRECTORY_TEST_EXCLUDE: set[str] = set()

UNIQUE_KEYS = {
    "microdatos_individuo": [
        "ano",
        "trimestre",
        "id_vivienda",
        "nro_hogar",
        "componente",
    ],
    "microdatos_hogar": ["ano", "trimestre", "id_vivienda", "nro_hogar"],
}
NOT_NULL = {
    "microdatos_individuo": [
        "ano",
        "trimestre",
        "id_vivienda",
        "nro_hogar",
        "componente",
    ],
    "microdatos_hogar": ["ano", "trimestre", "id_vivienda", "nro_hogar"],
}
DESCRIPTIONS = {
    "microdatos_individuo": (
        "Microdatos de personas de la Encuesta Permanente de Hogares (EPH continua) "
        "del INDEC, con una fila por persona, hogar y trimestre. Cubre las 87 ondas "
        "trimestrales publicadas entre 2003 Q3 y 2026 Q1 para los 31 aglomerados "
        "urbanos relevados, aproximadamente el 70% de la poblacion urbana argentina. "
        "El cuestionario cambio a lo largo de la serie, por lo que la tabla es la "
        "union de todos los esquemas: una columna que una onda no pregunto aparece "
        "nula en esa onda, y la columna temporal_coverage de la arquitectura indica "
        "el periodo de vigencia de cada una. INDEC no relevo 2007 Q3 y no publico "
        "2015 Q3, 2015 Q4 ni 2016 Q1; las series entre 2007 y 2015 estan sujetas a "
        "la advertencia oficial de INDEC sobre el periodo de intervencion. Use "
        "siempre pondera como factor de expansion. El identificador de vivienda "
        "id_vivienda cambia de formato en 2016, por lo que no permite seguir una "
        "vivienda a traves de ese corte."
    ),
    "microdatos_hogar": (
        "Microdatos de hogares de la Encuesta Permanente de Hogares (EPH continua) "
        "del INDEC, con una fila por hogar y trimestre. Cubre las 87 ondas "
        "trimestrales publicadas entre 2003 Q3 y 2026 Q1 para los 31 aglomerados "
        "urbanos relevados. Incluye caracteristicas habitacionales de la vivienda, "
        "estrategias de ingreso del hogar, ingreso total familiar y per capita, y la "
        "organizacion de las tareas domesticas. Se vincula con microdatos_individuo "
        "por id_vivienda y nro_hogar. La tabla es la union de todos los esquemas de "
        "cuestionario de la serie, por lo que una columna que una onda no pregunto "
        "aparece nula en esa onda. INDEC no relevo 2007 Q3 y no publico 2015 Q3, "
        "2015 Q4 ni 2016 Q1. Use siempre pondera como factor de expansion."
    ),
    "dicionario": (
        "Cobertura de diccionario para as colunas categoricas de ar_indec_eph. "
        "Traduz cada codigo armazenado nas tabelas de microdados para seu rotulo em "
        "espanhol, tal como publicado pelo INDEC. Os rotulos das ondas de 2003 a "
        "2015 vem das etiquetas de valor dos arquivos Stata; os das ondas de 2016 em "
        "diante vem do desenho de registros publicado pelo INDEC, ja que as bases "
        "TXT nao trazem etiquetas."
    ),
}


def arch(table: str) -> list[dict]:
    with open(ARCH_DIR / f"{table}.csv", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def cast(row: dict) -> str:
    name, btype = row["name"], row["bigquery_type"]
    return f"    safe_cast({name} as {btype.lower()}) {name},"


def model_sql(table: str, rows: list[dict]) -> str:
    casts = [cast(r) for r in rows]
    casts[-1] = casts[-1].rstrip(",")
    body = "\n".join(casts)
    return f"""{{{{
    config(
        alias="{table}",
        schema="{DATASET}",
        materialized="table",
        partition_by={{
            "field": "ano",
            "data_type": "int64",
            "range": {{
                "start": {PARTITION_START},
                "end": {PARTITION_END},
                "interval": 1,
            }},
        }},
        cluster_by=["trimestre", "id_aglomerado"],
    )
}}}}


select
{body}
from {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}} as t
"""


def dicionario_sql() -> str:
    return f"""{{{{
    config(
        alias="dicionario",
        schema="{DATASET}",
        materialized="table",
    )
}}}}


select
    safe_cast(id_tabela as string) id_tabela,
    safe_cast(nome_coluna as string) nome_coluna,
    safe_cast(chave as string) chave,
    safe_cast(cobertura_temporal as string) cobertura_temporal,
    safe_cast(valor as string) valor
from {{{{ set_datalake_project("{DATASET}_staging.dicionario") }}}} as t
"""


# yamlfix is the formatter of record for YAML in this repo, and it REFLOWS whole
# paragraphs inside a block scalar rather than wrapping line by line. Trying to
# match its output here is fragile, so this emits each description as a single
# unwrapped line and lets yamlfix do the wrapping. Run the hook after generating:
#
#     uv run python models/ar_indec_eph/code/gen_dbt.py
#     uv run pre-commit run --files models/ar_indec_eph/schema.yml
#
# The committed file is therefore yamlfix's output, not this script's, and
# regenerating dirties the wrapping until the hook runs again.
def yaml_block(text: str, indent: int) -> str:
    return " " * indent + " ".join(text.split())


def schema_yml(
    tables: dict[str, list[dict]], sparse: dict[str, list[str]]
) -> str:
    out = ["---", "version: 2", "models:"]
    for table, rows in tables.items():
        out.append(f"  - name: {DATASET}__{table}")
        out.append("    description: >")
        out.append(yaml_block(DESCRIPTIONS[table], 6))
        out.append("    tests:")
        out.append("      - dbt_utils.unique_combination_of_columns:")
        out.append(
            f"          combination_of_columns: [{', '.join(UNIQUE_KEYS[table])}]"
        )
        cols_sparse = sparse.get(table) or []
        out.append("      - not_null_proportion_multiple_columns:")
        out.append("          at_least: 0.05")
        if cols_sparse:
            out.append("          ignore_values:")
            out.extend(f"            - {c}" for c in cols_sparse)
        out.append("    columns:")
        for row in rows:
            out.append(f"      - name: {row['name']}")
            out.append("        description: >")
            out.append(yaml_block(row["description"], 12))
            tests: list[str] = []
            if row["name"] in NOT_NULL[table]:
                tests.append("          - not_null")
            if (
                row["directory_column"]
                and row["directory_column"].split(":")[0]
                not in DIRECTORY_TEST_EXCLUDE
            ):
                dataset_table, field = row["directory_column"].split(":")
                tests.append("          - relationships:")
                tests.append(
                    f"              to: ref('{dataset_table.replace('.', '__')}')"
                )
                # The field must be qualified as <table>.<column>. In these
                # directories the table is named after its key column, so a bare
                # "ano" makes BigQuery resolve the table itself as a STRUCT:
                #   No matching signature for operator = for argument types:
                #   INT64, STRUCT<ano INT64, bissexto INT64>
                # "ano.ano" is the form every other dataset in the repo uses.
                directory_table = dataset_table.split(".")[-1]
                tests.append(f"              field: {directory_table}.{field}")
            if tests:
                out.append("        tests:")
                out.extend(tests)
    # dicionario
    out.append(f"  - name: {DATASET}__dicionario")
    out.append("    description: >")
    out.append(yaml_block(DESCRIPTIONS["dicionario"], 6))
    out.append("    tests:")
    out.append("      - dbt_utils.unique_combination_of_columns:")
    out.append(
        "          combination_of_columns: [id_tabela, nome_coluna, chave]"
    )
    out.append("    columns:")
    for col, desc in [
        ("id_tabela", "Nome da tabela de microdados a que se refere a linha"),
        ("nome_coluna", "Nome da coluna codificada"),
        ("chave", "Codigo armazenado na coluna"),
        ("cobertura_temporal", "Periodo de vigencia do codigo"),
        (
            "valor",
            "Rotulo do codigo, em espanhol, tal como publicado pelo INDEC",
        ),
    ]:
        out.append(f"      - name: {col}")
        out.append(f"        description: {desc}")
    return "\n".join(out) + "\n"


def main() -> int:
    tables = {t: arch(t) for t in TABLES}
    sparse_path = CODE_DIR / "sparse_columns.json"
    sparse = (
        json.loads(sparse_path.read_text(encoding="utf-8"))
        if sparse_path.exists()
        else {}
    )
    for table, rows in tables.items():
        path = MODEL_DIR / f"{DATASET}__{table}.sql"
        path.write_text(model_sql(table, rows), encoding="utf-8")
        print(f"wrote {path.name} ({len(rows)} columns)")
    path = MODEL_DIR / f"{DATASET}__dicionario.sql"
    path.write_text(dicionario_sql(), encoding="utf-8")
    print(f"wrote {path.name}")
    (MODEL_DIR / "schema.yml").write_text(
        schema_yml(tables, sparse), encoding="utf-8"
    )
    print("wrote schema.yml")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
