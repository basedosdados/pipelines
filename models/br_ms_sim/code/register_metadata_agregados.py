"""Atualiza os metadados do br_ms_sim para a PR das agregadas derivadas do microdados.

    uv run python models/br_ms_sim/code/register_metadata_agregados.py [dev|staging|prod]
        [--apply] [--latest AAAA-MM-DD] [--end-year AAAA]

Sem ``--apply`` só imprime o plano. Em prod, rodar depois do merge e do
table-approve: antes disso o site mostraria cobertura até 2026 numa tabela que
ainda para em 2019.

O que muda, nas quatro agregadas (municipio, municipio_causa,
municipio_causa_idade, municipio_causa_idade_sexo_raca):

- descrição da tabela (PT/EN/ES), fonte bruta "Dados", publishedBy/dataCleanedBy
- cobertura 1996-2019 -> 1996-<end-year>
- Update da tabela: entity=year, frequency=1, latest=<latest>
- nível de observação sem entidade (o de causa_basica) -> health_condition
- descrições, unidades e covered_by_dictionary das colunas
- ligação coluna -> nível de observação (sexo_raca não tinha nenhuma; cria o de idade)
- municipio_causa_idade.ano: STRING -> INT64

No microdados: cria ``id_municipio_residencia_datasus`` e atualiza a descrição
de ``id_municipio_residencia``.

Os ids são lidos do backend a cada execução, então rodar de novo atualiza em vez
de duplicar. As colunas são alteradas por mutações parciais (só os campos
enviados mudam); ``update_column`` não é usado porque os booleanos dele têm
default False e apagariam ``is_partition``. No fim, o script confere que nenhum
``isPartition`` mudou.
"""

from __future__ import annotations

import argparse
import datetime
import json
from typing import Any

from databasis_mcp.gql import _gql, _mut
from databasis_mcp.tools import metadata as bd_meta
from databasis_mcp.tools import write as bd_write

DATASET_SLUG = "sim"
RAW_SOURCE_NAME = "Dados"
START_YEAR = 1996

TABLE_DESCRIPTION = {
    "municipio": (
        "Número de óbitos por ano e município de residência. Calculado a partir dos "
        "microdados. Óbitos sem município de residência informado entram com "
        "id_municipio nulo.",
        "Number of deaths by year and municipality of residence. Computed from the "
        "microdata. Deaths without a reported municipality of residence are included "
        "with a null id_municipio.",
        "Número de defunciones por año y municipio de residencia. Calculado a partir "
        "de los microdatos. Las defunciones sin municipio de residencia informado se "
        "incluyen con id_municipio nulo.",
    ),
    "municipio_causa": (
        "Número de óbitos por ano, município de residência e causa básica. Calculado "
        "a partir dos microdados. Óbitos sem município de residência informado entram "
        "com id_municipio nulo.",
        "Number of deaths by year, municipality of residence and underlying cause. "
        "Computed from the microdata. Deaths without a reported municipality of "
        "residence are included with a null id_municipio.",
        "Número de defunciones por año, municipio de residencia y causa básica. "
        "Calculado a partir de los microdatos. Las defunciones sin municipio de "
        "residencia informado se incluyen con id_municipio nulo.",
    ),
    "municipio_causa_idade": (
        "Número de óbitos por ano, município de residência, causa básica e idade. "
        "Calculado a partir dos microdados. Óbitos sem município de residência "
        "informado entram com id_municipio nulo.",
        "Number of deaths by year, municipality of residence, underlying cause and "
        "age. Computed from the microdata. Deaths without a reported municipality of "
        "residence are included with a null id_municipio.",
        "Número de defunciones por año, municipio de residencia, causa básica y edad. "
        "Calculado a partir de los microdatos. Las defunciones sin municipio de "
        "residencia informado se incluyen con id_municipio nulo.",
    ),
    "municipio_causa_idade_sexo_raca": (
        "Número de óbitos por ano, município de residência, causa básica, idade, sexo "
        "e raça ou cor, calculado a partir dos microdados. Óbitos sem município de "
        "residência informado entram com id_municipio nulo.",
        "Number of deaths by year, municipality of residence, underlying cause, age, "
        "sex and race or color, computed from the microdata. Deaths without a "
        "reported municipality of residence are included with a null id_municipio.",
        "Número de defunciones por año, municipio de residencia, causa básica, edad, "
        "sexo y raza o color, calculado a partir de los microdatos. Las defunciones "
        "sin municipio de residencia informado se incluyen con id_municipio nulo.",
    ),
}

# name: (pt, en, es, measurement_unit, covered_by_dictionary, entity do nível de observação)
AGG_COLUMNS: dict[str, tuple[str, str, str, str, bool | None, str | None]] = {
    "ano": ("Ano", "Year", "Año", "", None, "year"),
    "sigla_uf": (
        "Sigla da Unidade da Federação de residência",
        "Abbreviation of the Federative Unit of residence",
        "Sigla de la Unidad de la Federación de residencia",
        "",
        None,
        None,
    ),
    "id_municipio": (
        "ID Município de residência - IBGE 7 Dígitos (nulo se ignorado)",
        "Municipality of residence ID - IBGE 7 digits (null if unknown)",
        "ID del municipio de residencia - IBGE 7 dígitos (nulo si se ignora)",
        "",
        None,
        "municipality",
    ),
    "causa_basica": (
        "Causa básica (CID-10)",
        "Underlying cause of death (ICD-10)",
        "Causa básica de la defunción (CIE-10)",
        "",
        None,
        "health_condition",
    ),
    "idade": (
        "Idade em anos completos",
        "Age in completed years",
        "Edad en años cumplidos",
        "year",
        None,
        "age",
    ),
    "sexo": ("Sexo", "Sex", "Sexo", "", False, "sex"),
    "raca_cor": (
        "Raça ou cor",
        "Race or color",
        "Raza o color",
        "",
        False,
        "race",
    ),
    "numero_obitos": (
        "Número de óbitos",
        "Number of deaths",
        "Número de defunciones",
        "person",
        None,
        None,
    ),
}

MICRODADOS_COLUMNS = [
    {
        "name": "id_municipio_residencia",
        "description_pt": "ID Município de Residência - IBGE 7 Dígitos. Códigos da "
        "fonte sem equivalente no IBGE (município ignorado) ficam nulos",
        "description_en": "Municipality of residence ID - IBGE 7 digits. Source codes "
        "with no IBGE equivalent (unknown municipality) are null",
        "description_es": "ID del municipio de residencia - IBGE 7 dígitos. Los "
        "códigos de la fuente sin equivalente en el IBGE (municipio ignorado) quedan "
        "nulos",
    },
    {
        "name": "id_municipio_residencia_datasus",
        "bigquery_type": "STRING",
        "covered_by_dictionary": False,
        "has_sensitive_data": False,
        "description_pt": "ID Município de Residência como veio da fonte (DATASUS). "
        "Até 1998 inclui subdivisões das capitais (3345xxx Rio de Janeiro, 3580xxx São "
        "Paulo) e, até 2005, municípios ignorados (UF seguida de 00000)",
        "description_en": "Municipality of residence ID as published by the source "
        "(DATASUS). Up to 1998 it includes subdivisions of state capitals (3345xxx Rio "
        "de Janeiro, 3580xxx São Paulo) and, up to 2005, unknown municipalities (state "
        "code followed by 00000)",
        "description_es": "ID del municipio de residencia tal como lo publica la "
        "fuente (DATASUS). Hasta 1998 incluye subdivisiones de las capitales (3345xxx "
        "Río de Janeiro, 3580xxx São Paulo) y, hasta 2005, municipios ignorados (UF "
        "seguida de 00000)",
    },
]

TABLE_QUERY = """
query($id: ID!) {
  allTable(id: $id) { edges { node {
    _id slug namePt nameEn nameEs
    publishedBy { edges { node { id } } }
    dataCleanedBy { edges { node { id } } }
    observationLevels { edges { node { _id entity { slug } } } }
    coverages { edges { node { _id datetimeRanges { edges { node { _id } } } } } }
    updates { edges { node { _id } } }
  } } }
  allColumn(table_Id: $id) { edges { node {
    _id name order isPartition bigqueryType { name }
    observationLevel { _id }
  } } }
}
"""


def edges(conn: dict | None) -> list[dict]:
    return [e["node"] for e in (conn or {}).get("edges", [])]


class Runner:
    def __init__(self, env: str, apply: bool):
        self.env = env
        self.apply = apply
        self.steps = 0

    def do(self, label: str, fn, *args, **kwargs) -> Any:
        self.steps += 1
        shown = {k: v for k, v in kwargs.items() if k != "env"}
        print(
            f"[{self.steps:02d}] {label}: {json.dumps(shown, ensure_ascii=False)[:300]}"
        )
        if not self.apply:
            return None
        return fn(*args, env=self.env, **kwargs)

    def column(self, table_id: str, col: dict, **fields) -> Any:
        """Mutação parcial: só os campos passados mudam."""
        payload = {
            "id": col["_id"],
            "name": col["name"],
            "table": table_id,
            **fields,
        }
        return self.do(
            f"coluna {col['name']}",
            lambda env, **kw: _mut(
                "CreateUpdateColumn", kw["input"], "column { id }", env=env
            ),
            input=payload,
        )


def check_upsert(slug: str, result: dict | None) -> None:
    """bulk_upsert_columns devolve os erros em vez de levantar exceção."""
    if result and result.get("errors"):
        raise SystemExit(
            f"{slug}: bulk_upsert_columns falhou: {result['errors']}"
        )


def read_table(table_id: str, env: str) -> tuple[dict, dict[str, dict]]:
    data = _gql(TABLE_QUERY, {"id": table_id}, env=env)
    table = data["allTable"]["edges"][0]["node"]
    cols = {c["name"]: c for c in edges(data["allColumn"])}
    return table, cols


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("env", choices=["dev", "staging", "prod"])
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--latest", default=datetime.date.today().isoformat())
    ap.add_argument("--end-year", type=int, default=2026)
    args = ap.parse_args()
    env = args.env
    run = Runner(env, args.apply)
    print(
        f"env={env} apply={args.apply} latest={args.latest} end_year={args.end_year}\n"
    )

    ds = bd_meta.get_dataset(DATASET_SLUG, env=env)
    if not ds.get("found"):
        raise SystemExit(f"dataset {DATASET_SLUG!r} não encontrado em {env}")
    me = bd_meta.get_authenticated_account(env=env)["id"]
    raw = {
        r["name"]: r["id"]
        for r in bd_write.get_raw_data_sources(DATASET_SLUG, env=env)
    }
    raw_id = raw[RAW_SOURCE_NAME]
    ref = bd_meta.discover_ids(env=env, keys=["status", "bigquery_type"])
    published = ref["status"]["published"]
    int64 = ref["bigquery_type"]["INT64"]
    entity = {
        slug: bd_meta.lookup_id("entity", slug, env=env)["id"]
        for slug in (
            "year",
            "municipality",
            "health_condition",
            "age",
            "sex",
            "race",
        )
    }
    partitions_before: dict[tuple[str, str], bool] = {}

    for slug, (desc_pt, desc_en, desc_es) in TABLE_DESCRIPTION.items():
        print(f"\n=== {slug}")
        table_id = ds["tables"][slug]["id"]
        table, cols = read_table(table_id, env)
        partitions_before.update(
            {(slug, n): c["isPartition"] for n, c in cols.items()}
        )

        pub = [
            n["id"].split(":")[-1] for n in edges(table["publishedBy"])
        ] or [me]
        cleaned = [
            n["id"].split(":")[-1] for n in edges(table["dataCleanedBy"])
        ] or [me]
        run.do(
            "tabela",
            bd_write.create_update_table,
            id=table_id,
            slug=slug,
            name_pt=table["namePt"],
            name_en=table["nameEn"],
            name_es=table["nameEs"],
            dataset_id=ds["id"],
            status_id=published,
            published_by_ids=pub,
            data_cleaned_by_ids=cleaned,
            description_pt=desc_pt,
            description_en=desc_en,
            description_es=desc_es,
            raw_data_source_ids=[raw_id],
        )

        coverages = edges(table["coverages"])
        if len(coverages) != 1:
            raise SystemExit(
                f"{slug}: esperava 1 cobertura, achei {len(coverages)}"
            )
        ranges = edges(coverages[0]["datetimeRanges"])
        if len(ranges) != 1:
            raise SystemExit(
                f"{slug}: esperava 1 intervalo, achei {len(ranges)}"
            )
        run.do(
            "cobertura",
            bd_write.create_update_datetime_range,
            id=ranges[0]["_id"],
            coverage_id=coverages[0]["_id"],
            start_year=START_YEAR,
            end_year=args.end_year,
            interval=1,
        )

        updates = edges(table["updates"])
        run.do(
            "update",
            bd_write.create_update_update,
            id=updates[0]["_id"] if updates else None,
            table_id=table_id,
            entity_id=entity["year"],
            frequency=1,
            latest=f"{args.latest}T00:00:00",
        )

        # Nível de observação por entidade; o sem entidade é o de causa_basica.
        ol_by_entity: dict[str, str] = {}
        for ol in edges(table["observationLevels"]):
            ent = (ol["entity"] or {}).get("slug")
            # Em prod a entidade "unknown" foi apagada e o nível ficou sem entidade;
            # em staging ela ainda existe.
            if ent in (None, "unknown"):
                run.do(
                    "nível sem entidade -> health_condition",
                    bd_write.create_update_observation_level,
                    id=ol["_id"],
                    table_id=table_id,
                    entity_id=entity["health_condition"],
                )
                ent = "health_condition"
            ol_by_entity[ent] = ol["_id"]

        names = set(cols)
        for name, (_, _, _, _, _, ent) in AGG_COLUMNS.items():
            if name in names and ent and ent not in ol_by_entity:
                res = run.do(
                    f"cria nível {ent}",
                    bd_write.create_update_observation_level,
                    table_id=table_id,
                    entity_id=entity[ent],
                )
                ol_by_entity[ent] = res["id"] if res else f"<novo {ent}>"

        rows = []
        for name, (pt, en, es, unit, cbd, _) in AGG_COLUMNS.items():
            if name not in names:
                continue
            row: dict[str, Any] = {
                "name": name,
                "description_pt": pt,
                "description_en": en,
                "description_es": es,
            }
            if unit:
                row["measurement_unit"] = unit
            if cbd is not None:
                row["covered_by_dictionary"] = cbd
            rows.append(row)
        check_upsert(
            slug,
            run.do(
                "colunas (descrições, unidades, dicionário)",
                bd_write.bulk_upsert_columns,
                table_id=table_id,
                columns_json=json.dumps(rows, ensure_ascii=False),
                update_only=True,
            ),
        )

        for name, (*_, ent) in AGG_COLUMNS.items():
            if name not in names or not ent:
                continue
            col = cols[name]
            want = ol_by_entity[ent]
            if (col["observationLevel"] or {}).get("_id") != want:
                run.column(table_id, col, observationLevel=want)

        if cols["ano"]["bigqueryType"]["name"] != "INT64":
            run.column(table_id, cols["ano"], bigqueryType=int64)

    print("\n=== microdados")
    micro_id = ds["tables"]["microdados"]["id"]
    check_upsert(
        "microdados",
        run.do(
            "colunas",
            bd_write.bulk_upsert_columns,
            table_id=micro_id,
            columns_json=json.dumps(MICRODADOS_COLUMNS, ensure_ascii=False),
        ),
    )
    if args.apply:
        _, cols = read_table(micro_id, env)
        order = [
            c["name"]
            for c in sorted(cols.values(), key=lambda c: c["order"] or 0)
        ]
        order.remove("id_municipio_residencia_datasus")
        order.insert(
            order.index("id_municipio_residencia") + 1,
            "id_municipio_residencia_datasus",
        )
        run.do(
            "ordem das colunas",
            bd_write.reorder_columns,
            table_id=micro_id,
            column_names=order,
        )

    if args.apply:
        print("\n=== conferência")
        changed = []
        for slug in TABLE_DESCRIPTION:
            _, cols = read_table(ds["tables"][slug]["id"], env)
            for name, col in cols.items():
                if partitions_before.get((slug, name)) != col["isPartition"]:
                    changed.append(f"{slug}.{name}")
                ent = AGG_COLUMNS.get(name, (None,) * 6)[5]
                if ent and not col["observationLevel"]:
                    changed.append(f"{slug}.{name} sem nível de observação")
        print("OK" if not changed else f"ATENÇÃO: {changed}")
    print(
        f"\n{run.steps} passos {'aplicados' if args.apply else 'planejados (dry-run)'}"
    )


if __name__ == "__main__":
    main()
