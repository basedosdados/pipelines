"""Register the br_cnj_datajud metadata in a Data Basis backend.

Columns come from the architecture CSVs, which are the source of truth. The
dataset is always written as under_review unless --publish is passed; on prod,
publish only after the PR has merged, table-approve has materialised the tables
and they have been verified.

    uv run python -m models.br_cnj_datajud.code.register_metadata --env prod
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
from pathlib import Path

import databasis_mcp.tools.metadata as bd_mcp_metadata
import databasis_mcp.tools.write as bd_mcp_write

SLUG = "datajud"
GCP_DATASET_ID = "br_cnj_datajud"
ARCH_DIR = Path(__file__).parent / "architecture"
TABLE_ORDER = ["tribunal_ano", "tribunal", "dicionario"]

# Prod tag slugs are English; staging holds Portuguese ones (burocracia,
# congestionamento, direito, judiciario, julgamento, magistrado, produtividade).
TAG_SLUGS = {
    "prod": [
        "bureaucracy",
        "congestion",
        "law",
        "judiciary",
        "judgment",
        "magistrate",
        "productivity",
    ],
}

DATASET = {
    "name_pt": "Justiça em Números (DataJud)",
    "name_en": "Justice in Numbers (DataJud)",
    "name_es": "Justicia en Números (DataJud)",
    "description_pt": (
        "Painel estatístico do Poder Judiciário brasileiro publicado anualmente "
        "pelo Conselho Nacional de Justiça no relatório Justiça em Números, com o "
        "DataJud como base de dados de origem desde 2022. Reúne 1.305 indicadores "
        "por tribunal e ano de referência entre 2009 e 2025, cobrindo 99 tribunais "
        "de todos os ramos de justiça, entre eles casos novos, casos pendentes, "
        "taxa de congestionamento, índice de atendimento à demanda e IPC-Jus, "
        "discriminados por grau e fase processual. Inclui também os atributos "
        "cadastrais de cada tribunal e o dicionário de códigos dos indicadores."
    ),
    "description_en": (
        "Statistical panel of the Brazilian Judiciary published annually by the "
        "National Council of Justice in the Justice in Numbers report, with "
        "DataJud as its source database since 2022. It gathers 1,305 indicators "
        "by court and reference year between 2009 and 2025, covering 99 courts "
        "across every branch of justice, among them new cases, pending cases, "
        "congestion rate, demand service index and IPC-Jus, broken down by "
        "instance and procedural phase. It also includes each court's registry "
        "attributes and the indicator code dictionary."
    ),
    "description_es": (
        "Panel estadístico del Poder Judicial brasileño publicado anualmente por "
        "el Consejo Nacional de Justicia en el informe Justicia en Números, con "
        "DataJud como base de datos de origen desde 2022. Reúne 1.305 "
        "indicadores por tribunal y año de referencia entre 2009 y 2025, "
        "cubriendo 99 tribunales de todas las ramas de justicia, entre ellos "
        "casos nuevos, casos pendientes, tasa de congestión, índice de atención "
        "a la demanda e IPC-Jus, desglosados por grado y fase procesal. Incluye "
        "además los atributos registrales de cada tribunal y el diccionario de "
        "códigos de los indicadores."
    ),
}

RAW_SOURCE = {
    "name_pt": "Base de Dados do Justiça em Números",
    "name_en": "Justice in Numbers Database",
    "name_es": "Base de Datos de Justicia en Números",
    "url": "https://www.cnj.jus.br/pesquisas-judiciarias/justica-em-numeros/base-de-dados/",
    "description_pt": (
        "Página do CNJ que distribui a base consolidada do Justiça em Números em "
        "arquivo ZIP, contendo o painel por tribunal e ano, a base da Justiça "
        "Federal por seção judiciária e a tabela de descrição das variáveis. O "
        "nome do arquivo incorpora a data da edição e muda a cada publicação anual"
    ),
    "description_en": (
        "CNJ page distributing the consolidated Justice in Numbers database as a "
        "ZIP file, containing the court-by-year panel, the Federal Justice base "
        "by judicial section and the variable description table. The file name "
        "embeds the release date and changes with every annual publication"
    ),
    "description_es": (
        "Página del CNJ que distribuye la base consolidada de Justicia en Números "
        "en archivo ZIP, con el panel por tribunal y año, la base de la Justicia "
        "Federal por sección judicial y la tabla de descripción de las variables. "
        "El nombre del archivo incorpora la fecha de la edición y cambia en cada "
        "publicación anual"
    ),
}
# Max coverage date of the source: the 2025 reference year.
RAW_SOURCE_LATEST = "2025-01-01T00:00:00-03:00"

TABLES = {
    "tribunal_ano": {
        "name_pt": "Indicadores por Tribunal e Ano",
        "name_en": "Indicators by Court and Year",
        "name_es": "Indicadores por Tribunal y Año",
        "description_pt": (
            "Painel estatístico do Justiça em Números em formato longo, com uma "
            "linha por tribunal, ano de referência, ramo de justiça e indicador. "
            "Cobre 1.305 indicadores e 99 tribunais entre 2009 e 2025, incluindo "
            "casos novos, casos pendentes, taxa de congestionamento, índice de "
            "atendimento à demanda e IPC-Jus, discriminados por grau e fase "
            "processual no sufixo do código do indicador. Os valores ausentes "
            "sinalizados na fonte como nd, n/a e Inf foram convertidos em nulo e "
            "as linhas correspondentes não foram materializadas"
        ),
        "description_en": (
            "Justice in Numbers statistical panel in long format, with one row "
            "per court, reference year, branch of justice and indicator. It "
            "covers 1,305 indicators and 99 courts between 2009 and 2025, "
            "including new cases, pending cases, congestion rate, demand service "
            "index and IPC-Jus, broken down by instance and procedural phase in "
            "the indicator code suffix. Missing values flagged in the source as "
            "nd, n/a and Inf were converted to null and the corresponding rows "
            "were not materialised"
        ),
        "description_es": (
            "Panel estadístico de Justicia en Números en formato largo, con una "
            "fila por tribunal, año de referencia, rama de justicia e indicador. "
            "Cubre 1.305 indicadores y 99 tribunales entre 2009 y 2025, "
            "incluyendo casos nuevos, casos pendientes, tasa de congestión, "
            "índice de atención a la demanda e IPC-Jus, desglosados por grado y "
            "fase procesal en el sufijo del código del indicador. Los valores "
            "ausentes señalados en la fuente como nd, n/a e Inf se convirtieron "
            "en nulo y las filas correspondientes no se materializaron"
        ),
        "observation_levels": [("year", "ano"), ("court", "sigla_tribunal")],
    },
    "tribunal": {
        "name_pt": "Tribunais",
        "name_en": "Courts",
        "name_es": "Tribunales",
        "description_pt": (
            "Atributos cadastrais de cada tribunal reportados por ano de "
            "referência: nome por extenso, unidade da federação da sede, âmbito "
            "territorial de atuação, porte atribuído pelo CNJ e avaliação de "
            "estrutura. Separada da tabela de indicadores porque estas colunas "
            "são categóricas e não caberiam na coluna numérica de valor"
        ),
        "description_en": (
            "Registry attributes of each court reported by reference year: full "
            "name, federative unit of its seat, territorial scope of "
            "jurisdiction, size class assigned by CNJ and structure assessment. "
            "Kept separate from the indicator table because these columns are "
            "categorical and would not fit the numeric value column"
        ),
        "description_es": (
            "Atributos registrales de cada tribunal reportados por año de "
            "referencia: nombre completo, unidad federativa de la sede, ámbito "
            "territorial de actuación, tamaño asignado por el CNJ y evaluación "
            "de estructura. Separada de la tabla de indicadores porque estas "
            "columnas son categóricas y no cabrían en la columna numérica de valor"
        ),
        "observation_levels": [("year", "ano"), ("court", "sigla_tribunal")],
    },
    "dicionario": {
        "name_pt": "Dicionário",
        "name_en": "Dictionary",
        "name_es": "Diccionario",
        "description_pt": (
            "Descrição de cada código de indicador usado na tabela tribunal_ano, "
            "conforme a tabela de variáveis que o CNJ distribui junto com a base "
            "de dados. Cobre os 1.305 códigos presentes na série"
        ),
        "description_en": (
            "Description of each indicator code used in the tribunal_ano table, "
            "as given in the variable table CNJ distributes alongside the "
            "database. It covers the 1,305 codes present in the series"
        ),
        "description_es": (
            "Descripción de cada código de indicador usado en la tabla "
            "tribunal_ano, según la tabla de variables que el CNJ distribuye "
            "junto con la base de datos. Cubre los 1.305 códigos presentes en la "
            "serie"
        ),
        "observation_levels": [],
    },
}
COVERAGE_YEARS = (2009, 2025)


def as_dict(result):
    return json.loads(result) if isinstance(result, str) else result


def bare_id(node_id: str) -> str:
    return node_id.split(":", 1)[1] if ":" in node_id else node_id


def arch_columns(slug: str) -> list[dict]:
    with (ARCH_DIR / f"{slug}.csv").open(encoding="utf-8") as f:
        return list(csv.DictReader(f))


def columns_payload(slug: str) -> list[dict]:
    return [
        {
            "name": r["name"],
            "bigquery_type": r["bigquery_type"],
            "description": r["description_pt"],
            "description_en": r["description_en"],
            "description_es": r["description_es"],
            "covered_by_dictionary": r["covered_by_dictionary"] == "yes",
            "directory_column": r["directory_column"],
            "measurement_unit": r["measurement_unit"],
            "has_sensitive_data": r["has_sensitive_data"] == "yes",
            "observations": r["observations_pt"],
            "observations_en": r["observations_en"],
            "observations_es": r["observations_es"],
            "is_partition": r["name"] == "ano",
            "order": order,
        }
        for order, r in enumerate(arch_columns(slug))
    ]


def area_id(slug: str, env: str) -> str:
    q = f'query {{ allArea(slug: "{slug}") {{ edges {{ node {{ id }} }} }} }}'
    edges = bd_mcp_metadata._gql(q, {}, env=env)["allArea"]["edges"]
    return bare_id(edges[0]["node"]["id"])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--env", default="staging", choices=["staging", "dev", "prod"]
    )
    ap.add_argument("--publish", action="store_true")
    args = ap.parse_args()
    env = args.env

    ids = as_dict(
        bd_mcp_metadata.discover_ids(
            env=env,
            keys=[
                "status",
                "license",
                "availability",
                "entity",
                "organization",
                "theme",
                "tag",
            ],
        )
    )
    account = as_dict(bd_mcp_metadata.get_authenticated_account(env=env))
    account_id = account.get("id") or account["account"]["id"]

    missing = [t for t in TAG_SLUGS.get(env, []) if t not in ids["tag"]]
    if missing:
        raise SystemExit(f"tags not found in {env}: {missing}")
    tag_ids = [ids["tag"][t] for t in TAG_SLUGS.get(env, [])]

    prior = as_dict(bd_mcp_metadata.get_dataset(SLUG, env=env)) or {}
    status = "published" if args.publish else "under_review"
    dataset = as_dict(
        bd_mcp_write.create_update_dataset(
            slug=SLUG,
            organization_ids=[ids["organization"]["cnj"]],
            theme_ids=[ids["theme"]["government"], ids["theme"]["justice"]],
            tag_ids=tag_ids,
            status_id=ids["status"][status],
            id=prior.get("id"),
            env=env,
            **DATASET,
        )
    )
    dataset_id = dataset.get("id") or prior["id"]
    print(f"dataset {SLUG} -> {dataset_id} ({status}), {len(tag_ids)} tags")

    raws = as_dict(bd_mcp_write.get_raw_data_sources(SLUG, env=env)) or []
    raw_prev = next(
        (
            r["id"]
            for r in raws
            if isinstance(r, dict) and r.get("url") == RAW_SOURCE["url"]
        ),
        None,
    )
    raw = as_dict(
        bd_mcp_write.create_update_raw_data_source(
            dataset_id=dataset_id,
            license_id=ids["license"]["unknown"],
            availability_id=ids["availability"]["online"],
            has_structured_data=True,
            contains_api=False,
            is_free=True,
            requires_registration=False,
            id=raw_prev,
            env=env,
            **RAW_SOURCE,
        )
    )
    raw_id = raw.get("id") or raw_prev
    if not raw_id:
        raise SystemExit(f"raw data source not created: {raw}")
    print(f"raw data source -> {raw_id}")
    if raw_prev is None:
        bd_mcp_write.create_update_update(
            entity_id=ids["entity"]["year"],
            frequency=1,
            latest=RAW_SOURCE_LATEST,
            raw_data_source_id=raw_id,
            env=env,
        )

    br = area_id("br", env)
    now = dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat()
    state = as_dict(bd_mcp_metadata.get_dataset(SLUG, env=env))
    for slug in TABLE_ORDER:
        spec = TABLES[slug]
        prev = state["tables"].get(slug, {})
        table = as_dict(
            bd_mcp_write.create_update_table(
                slug=slug,
                name_pt=spec["name_pt"],
                name_en=spec["name_en"],
                name_es=spec["name_es"],
                description_pt=spec["description_pt"],
                description_en=spec["description_en"],
                description_es=spec["description_es"],
                dataset_id=dataset_id,
                status_id=ids["status"]["published"],
                published_by_ids=[account_id],
                data_cleaned_by_ids=[account_id],
                raw_data_source_ids=[raw_id],
                id=prev.get("id"),
                env=env,
            )
        )
        table_id = table.get("id") or prev["id"]

        # Children are created only when absent: create_update_* without an id
        # duplicates them on every run.
        ols = {
            o["entity_slug"]: o["id"]
            for o in prev.get("observation_levels", [])
        }
        for entity, _ in spec["observation_levels"]:
            if entity not in ols:
                ol = as_dict(
                    bd_mcp_write.create_update_observation_level(
                        table_id=table_id,
                        entity_id=ids["entity"][entity],
                        env=env,
                    )
                )
                ols[entity] = ol["id"]
        if spec["observation_levels"]:
            bd_mcp_write.reorder_observation_levels(
                table_id=table_id,
                ol_ids=[ols[e] for e, _ in spec["observation_levels"]],
                env=env,
            )

        result = as_dict(
            bd_mcp_write.bulk_upsert_columns(
                table_id=table_id,
                columns_json=json.dumps(
                    columns_payload(slug), ensure_ascii=False
                ),
                env=env,
            )
        )
        if result.get("errors"):
            raise SystemExit(f"{slug}: column errors {result['errors']}")
        bd_mcp_write.reorder_columns(
            table_id=table_id,
            column_names=[r["name"] for r in arch_columns(slug)],
            env=env,
        )

        # bulk_upsert does not write is_partition or the observation level.
        cols = {
            c["name"]: c["id"]
            for c in as_dict(bd_mcp_metadata.get_dataset(SLUG, env=env))[
                "tables"
            ][slug]["columns"]
        }
        arch = {r["name"]: r for r in arch_columns(slug)}
        ol_by_col = {col: ols[e] for e, col in spec["observation_levels"]}
        for name in sorted({"ano", *ol_by_col} & set(cols)):
            r = arch[name]
            bd_mcp_write.update_column(
                column_id=cols[name],
                column_name=name,
                table_id=table_id,
                observation_level_id=ol_by_col.get(name),
                is_partition=name == "ano",
                description_pt=r["description_pt"],
                description_en=r["description_en"],
                description_es=r["description_es"],
                measurement_unit=r["measurement_unit"],
                covered_by_dictionary=r["covered_by_dictionary"] == "yes",
                directory_column_name=r["directory_column"],
                observations_pt=r["observations_pt"],
                observations_en=r["observations_en"],
                observations_es=r["observations_es"],
                env=env,
            )

        clouds = prev.get("cloud_tables", [])
        bd_mcp_write.create_update_cloud_table(
            table_id=table_id,
            gcp_project_id="basedosdados"
            if env == "prod"
            else "basedosdados-dev",
            gcp_dataset_id=GCP_DATASET_ID,
            gcp_table_id=slug,
            id=clouds[0]["id"] if clouds else None,
            env=env,
        )

        coverages = prev.get("coverages", [])
        if coverages:
            coverage_id = coverages[0]["id"]
        else:
            coverage_id = as_dict(
                bd_mcp_write.create_update_coverage(
                    table_id=table_id, area_id=br, is_closed=False, env=env
                )
            )["id"]
        ranges = coverages[0]["datetime_ranges"] if coverages else []
        bd_mcp_write.create_update_datetime_range(
            coverage_id=coverage_id,
            start_year=COVERAGE_YEARS[0],
            end_year=COVERAGE_YEARS[1],
            interval=1,
            id=ranges[0]["id"] if ranges else None,
            env=env,
        )

        updates = prev.get("updates", [])
        bd_mcp_write.create_update_update(
            entity_id=ids["entity"]["year"],
            frequency=1,
            lag=1,
            latest=now,
            table_id=table_id,
            id=updates[0]["id"] if updates else None,
            env=env,
        )
        print(
            f"table {slug} -> {table_id}: {len(cols)} columns, OLs {list(ols)}"
        )

    bd_mcp_write.reorder_tables(
        dataset_slug=SLUG, table_slugs=TABLE_ORDER, env=env
    )


if __name__ == "__main__":
    main()
