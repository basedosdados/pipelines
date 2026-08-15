"""
Generate dbt models (.sql) + schema.yml for br_sfb_sicar from architecture.py.
Nine spatial theme tables (incremental, partition by data (snapshot) DATE,
cluster sigla_uf) + the dictionary (table).
"""

import os

import architecture as A  # noqa: N812
import yaml

MODELS_DIR = os.path.join(os.path.dirname(__file__), "..")
DS = "br_sfb_sicar"

TABLE_DESC = {
    "area_imovel": "Perímetro e atributos declaratórios dos imóveis rurais cadastrados no Cadastro Ambiental Rural (CAR/SICAR). Uma linha por imóvel por snapshot da base.",
    "app": "Áreas de Preservação Permanente (APP) declaradas nos imóveis do CAR/SICAR.",
    "reserva_legal": "Áreas de Reserva Legal declaradas nos imóveis do CAR/SICAR.",
    "vegetacao_nativa": "Remanescentes de vegetação nativa declarados nos imóveis do CAR/SICAR.",
    "area_consolidada": "Áreas rurais consolidadas declaradas nos imóveis do CAR/SICAR.",
    "area_pousio": "Áreas de pousio declaradas nos imóveis do CAR/SICAR.",
    "uso_restrito": "Áreas de uso restrito declaradas nos imóveis do CAR/SICAR.",
    "servidao_administrativa": "Áreas de servidão administrativa declaradas nos imóveis do CAR/SICAR.",
    "hidrografia": "Feições de hidrografia declaradas nos imóveis do CAR/SICAR.",
}


def cast(col):
    name, typ = col["name"], col["type"]
    if typ == "GEOGRAPHY":
        return f"    safe.st_geogfromtext({name}, make_valid => true) {name},"
    if typ == "DATE":
        return f"    safe_cast({name} as date) {name},"
    if typ == "FLOAT64":
        return f"    safe_cast({name} as float64) {name},"
    return f"    safe_cast({name} as string) {name},"


def model_sql(table):
    cols = A.TABLES[table]
    selects = "\n".join(cast(c) for c in cols)
    return f"""{{{{
    config(
        alias="{table}",
        schema="{DS}",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={{
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        }},
        cluster_by=["sigla_uf"],
    )
}}}}

select
{selects}
from {{{{ set_datalake_project("{DS}_staging.{table}") }}}} as t
{{% if is_incremental() %}}
    where data > (select max(data) from {{{{ this }}}})
{{% endif %}}
"""


def schema_model(table):
    cols = A.TABLES[table]
    is_area = table == "area_imovel"
    tests = []
    if is_area:
        # A handful of properties are duplicated in the source shapefile
        # (~0.02%); tolerate a tiny proportion rather than dropping rows.
        tests.append(
            {
                "custom_unique_combinations_of_columns": {
                    "combination_of_columns": ["data", "id_imovel"],
                    "proportion_allowed_failures": 0.005,
                }
            }
        )
    tests.append({"not_null_proportion_multiple_columns": {"at_least": 0.05}})
    tests.append(
        {
            "custom_dictionary_coverage": {
                "columns_covered_by_dictionary": ["status", "tipo"],
                "dictionary_model": f"ref('{DS}__dicionario')",
            }
        }
    )
    col_entries = []
    for c in cols:
        e = {"name": c["name"], "description": c["desc_pt"]}
        if c["name"] == "data":
            # directory table is named `data`; disambiguate table.column
            e["tests"] = [
                {
                    "relationships": {
                        "to": "ref('br_bd_diretorios_data_tempo__data')",
                        "field": "data.data",
                    }
                }
            ]
        elif c["name"] == "sigla_uf":
            e["tests"] = [
                {
                    "relationships": {
                        "to": "ref('br_bd_diretorios_brasil__uf')",
                        "field": "sigla",
                    }
                }
            ]
        elif c["name"] == "id_municipio":
            e["tests"] = [
                {
                    "relationships": {
                        "to": "ref('br_bd_diretorios_brasil__municipio')",
                        "field": "id_municipio",
                    }
                }
            ]
        col_entries.append(e)
    return {
        "name": f"{DS}__{table}",
        "description": TABLE_DESC[table],
        "tests": tests,
        "columns": col_entries,
    }


def schema_dicionario():
    cols = A.DICIONARIO
    return {
        "name": f"{DS}__dicionario",
        "description": "Dicionário para tradução dos códigos das tabelas do conjunto br_sfb_sicar.",
        "tests": [
            {
                "dbt_utils.unique_combination_of_columns": {
                    "combination_of_columns": [
                        "id_tabela",
                        "nome_coluna",
                        "chave",
                        "cobertura_temporal",
                    ]
                }
            }
        ],
        "columns": [
            {"name": c["name"], "description": c["desc_pt"]} for c in cols
        ],
    }


def dicionario_sql():
    return f"""{{{{
    config(
        alias="dicionario",
        schema="{DS}",
        materialized="table",
    )
}}}}

select
    safe_cast(id_tabela as string) id_tabela,
    safe_cast(nome_coluna as string) nome_coluna,
    safe_cast(chave as string) chave,
    safe_cast(cobertura_temporal as string) cobertura_temporal,
    safe_cast(valor as string) valor,
from {{{{ set_datalake_project("{DS}_staging.dicionario") }}}} as t
"""


class _Dumper(yaml.SafeDumper):
    pass


def _str_presenter(dumper, data):
    if len(data) > 80 or ":" in data:
        return dumper.represent_scalar(
            "tag:yaml.org,2002:str", data, style=">"
        )
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


_Dumper.add_representer(str, _str_presenter)


def main():
    for table in A.TABLES:
        with open(os.path.join(MODELS_DIR, f"{DS}__{table}.sql"), "w") as f:
            f.write(model_sql(table))
    with open(os.path.join(MODELS_DIR, f"{DS}__dicionario.sql"), "w") as f:
        f.write(dicionario_sql())

    models = [schema_model(t) for t in A.TABLES] + [schema_dicionario()]
    schema = {"version": 2, "models": models}
    with open(os.path.join(MODELS_DIR, "schema.yml"), "w") as f:
        f.write("---\n")
        yaml.dump(
            schema,
            f,
            Dumper=_Dumper,
            sort_keys=False,
            allow_unicode=True,
            default_flow_style=False,
            width=88,
        )
    print("generated", len(A.TABLES), "theme models + dicionario + schema.yml")


if __name__ == "__main__":
    main()
