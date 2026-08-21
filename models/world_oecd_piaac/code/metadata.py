"""Register world_oecd_piaac metadata in the Data Basis backend.

Usage:
    uv run python models/world_oecd_piaac/code/metadata.py --env staging [--publish]

Runs through the databasis MCP module directly rather than the MCP server, because
the server process caches its code and does not yet expose auxiliary_files_url.

Language note: PIAAC publishes its variable labels in English only. The ~2,000
column descriptions taken from the codebooks are therefore the OECD's own English
text in all three language fields, rather than machine translations of technical
survey items, which would put errors into metadata people rely on. Descriptions
written here -- the dataset, the tables, and the grain, item, variable and
dictionary columns -- are properly translated. This mirrors world_oecd_pisa,
which also stores English in descriptionPt.
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import sys
from pathlib import Path

CODE_DIR = Path(__file__).parent
sys.path.insert(0, str(CODE_DIR))

import architecture as arch  # noqa: E402

_spec = importlib.util.spec_from_file_location(
    "bdsrv", Path.home() / "Dropbox" / "BD" / "mcp" / "server.py"
)
BD = importlib.util.module_from_spec(_spec)
sys.modules["bdsrv"] = BD
_spec.loader.exec_module(BD)


def tool(fn):
    return getattr(fn, "fn", fn)


DATASET_SLUG = "piaac"
GCP_DATASET = "world_oecd_piaac"
BUCKET_URL = "https://storage.googleapis.com/{bucket}/auxiliary_files/{ds}/{table}/auxiliary_files.zip"

DATASET_NAME = {
    "pt": "Pesquisa Internacional de Competências de Adultos (PIAAC)",
    "en": "Survey of Adult Skills (PIAAC)",
    "es": "Encuesta Internacional de Competencias de Adultos (PIAAC)",
}
DATASET_DESCRIPTION = {
    "pt": (
        "Microdados da Pesquisa Internacional de Competências de Adultos (PIAAC) da "
        "OCDE, que mede letramento, numeramento e resolução de problemas em adultos "
        "de 16 a 65 anos. Reúne os arquivos de uso público dos dois ciclos: 35 "
        "arquivos internacionais do Ciclo 1, coletados em três rodadas entre 2011 e "
        "2017, e 30 arquivos do Ciclo 2, coletados entre setembro de 2022 e agosto "
        "de 2023. Cada ciclo é dividido em uma tabela por respondente e uma tabela "
        "longa de respostas por item."
    ),
    "en": (
        "Microdata from the OECD Survey of Adult Skills (PIAAC), which measures "
        "literacy, numeracy and problem solving among adults aged 16 to 65. Covers "
        "the Public Use Files of both cycles: 35 internationally comparable Cycle 1 "
        "files collected across three rounds between 2011 and 2017, and 30 Cycle 2 "
        "files collected between September 2022 and August 2023. Each cycle is split "
        "into a respondent-level table and a long table of item responses."
    ),
    "es": (
        "Microdatos de la Encuesta Internacional de Competencias de Adultos (PIAAC) "
        "de la OCDE, que mide la comprensión lectora, las competencias matemáticas y "
        "la resolución de problemas en adultos de 16 a 65 años. Incluye los archivos "
        "de uso público de ambos ciclos: 35 archivos internacionales del Ciclo 1, "
        "recogidos en tres rondas entre 2011 y 2017, y 30 archivos del Ciclo 2, "
        "recogidos entre septiembre de 2022 y agosto de 2023. Cada ciclo se divide en "
        "una tabla por encuestado y una tabla larga de respuestas por ítem."
    ),
}

TABLE_NAMES = {
    "respondent_cycle_1": (
        "Respondente - Ciclo 1",
        "Respondent - Cycle 1",
        "Encuestado - Ciclo 1",
    ),
    "respondent_cycle_2": (
        "Respondente - Ciclo 2",
        "Respondent - Cycle 2",
        "Encuestado - Ciclo 2",
    ),
    "respondent_cycle_1_usa_national": (
        "Respondente - Ciclo 1 - Estados Unidos (arquivo nacional)",
        "Respondent - Cycle 1 - United States (national file)",
        "Encuestado - Ciclo 1 - Estados Unidos (archivo nacional)",
    ),
    "item_response_cycle_1": (
        "Resposta por item - Ciclo 1",
        "Item response - Cycle 1",
        "Respuesta por ítem - Ciclo 1",
    ),
    "item_response_cycle_2": (
        "Resposta por item - Ciclo 2",
        "Item response - Cycle 2",
        "Respuesta por ítem - Ciclo 2",
    ),
    "variable": ("Variáveis", "Variables", "Variables"),
    "dictionary": ("Dicionário", "Dictionary", "Diccionario"),
}

RAW_SOURCES = [
    (
        "puf_cycle_1",
        (
            "Arquivos de uso público - Ciclo 1",
            "Public Use Files - Cycle 1",
            "Archivos de uso público - Ciclo 1",
        ),
        "https://webfs.oecd.org/piaac/cy1-puf-data/CSV/",
        (
            "Arquivos de uso público do Ciclo 1 em CSV, um por país. A página de download "
            "da OCDE pede cadastro, mas os arquivos são servidos sem autenticação.",
            "Cycle 1 Public Use Files in CSV, one per country. The OECD download page asks "
            "for registration, but the files themselves are served without authentication.",
            "Archivos de uso público del Ciclo 1 en CSV, uno por país. La página de descarga "
            "de la OCDE pide registro, pero los archivos se sirven sin autenticación.",
        ),
    ),
    (
        "puf_cycle_2",
        (
            "Arquivos de uso público - Ciclo 2",
            "Public Use Files - Cycle 2",
            "Archivos de uso público - Ciclo 2",
        ),
        "https://webfs.oecd.org/piaac/cy2-puf-data/CSV/",
        (
            "Arquivos de uso público do Ciclo 2 em CSV, um por país. O arquivo dos Países "
            "Baixos está pendente de autorização nacional.",
            "Cycle 2 Public Use Files in CSV, one per country. The Netherlands file is "
            "pending national authorisation.",
            "Archivos de uso público del Ciclo 2 en CSV, uno por país. El archivo de los "
            "Países Bajos está pendiente de autorización nacional.",
        ),
    ),
    (
        "database_cycle_1",
        (
            "Base de dados e materiais - Ciclo 1",
            "Database and materials - Cycle 1",
            "Base de datos y materiales - Ciclo 1",
        ),
        "https://www.oecd.org/en/data/datasets/piaac-1st-cycle-database.html",
        (
            "Codebooks, questionários, compêndios e demais materiais do Ciclo 1.",
            "Codebooks, questionnaires, compendia and other Cycle 1 materials.",
            "Codebooks, cuestionarios, compendios y demás materiales del Ciclo 1.",
        ),
    ),
    (
        "database_cycle_2",
        (
            "Base de dados e materiais - Ciclo 2",
            "Database and materials - Cycle 2",
            "Base de datos y materiales - Ciclo 2",
        ),
        "https://www.oecd.org/en/data/datasets/piaac-2nd-cycle-database.html",
        (
            "Codebooks, questionários, compêndios e demais materiais do Ciclo 2.",
            "Codebooks, questionnaires, compendia and other Cycle 2 materials.",
            "Codebooks, cuestionarios, compendios y demás materiales del Ciclo 2.",
        ),
    ),
]

# Table -> the raw source it is linked to. At most one per table: the backend
# client raises when a table has two or more (see prefect-pipeline-conventions).
TABLE_SOURCE = {
    "respondent_cycle_1": "puf_cycle_1",
    "respondent_cycle_1_usa_national": "puf_cycle_1",
    "item_response_cycle_1": "puf_cycle_1",
    "respondent_cycle_2": "puf_cycle_2",
    "item_response_cycle_2": "puf_cycle_2",
    "variable": "database_cycle_2",
    "dictionary": "database_cycle_2",
}

# Descriptions written here, translated properly. Everything else keeps the
# OECD's English label verbatim.
TRANSLATED: dict[str, tuple[str, str]] = {
    "year": (
        "Ano de referência da coleta, derivado do ciclo e da rodada da pesquisa",
        "Año de referencia de la recogida, derivado del ciclo y la ronda de la encuesta",
    ),
    "cycle": (
        "Ciclo da Pesquisa Internacional de Competências de Adultos (1 ou 2)",
        "Ciclo de la Encuesta Internacional de Competencias de Adultos (1 o 2)",
    ),
    "round": (
        "Rodada de coleta dentro do ciclo",
        "Ronda de recogida dentro del ciclo",
    ),
    "country_id_iso_3": (
        "Código ISO 3166-1 alfa-3 do país participante",
        "Código ISO 3166-1 alfa-3 del país participante",
    ),
    "country_id_m49": (
        "Código numérico M49 da ONU do país participante",
        "Código numérico M49 de la ONU del país participante",
    ),
    "country_entity_id": (
        "Código do país ou entidade subnacional participante, conforme informado pelo PIAAC",
        "Código del país o entidad subnacional participante, según lo informado por PIAAC",
    ),
    "respondent_id": (
        "Identificador sequencial do respondente, atribuído aleatoriamente",
        "Identificador secuencial del encuestado, asignado aleatoriamente",
    ),
    "item_code": (
        "Código do item da avaliação, compartilhado por todas as medidas registradas para ele",
        "Código del ítem de la evaluación, compartido por todas las medidas registradas para él",
    ),
    "domain": (
        "Domínio da avaliação ao qual o item pertence",
        "Dominio de la evaluación al que pertenece el ítem",
    ),
    "scored_response": (
        "Código da resposta pontuada, conforme registrado pelo PIAAC",
        "Código de la respuesta puntuada, según lo registrado por PIAAC",
    ),
    "scored_response_label": (
        "Significado de scored_response, decodificado com o esquema de pontuação do próprio item",
        "Significado de scored_response, decodificado con el esquema de puntuación del propio ítem",
    ),
    "raw_response": (
        "Resposta tal como digitada pelo respondente, nos itens que a registram",
        "Respuesta tal como la introdujo el encuestado, en los ítems que la registran",
    ),
    "timing_seconds": (
        "Tempo total que o respondente passou no item",
        "Tiempo total que el encuestado pasó en el ítem",
    ),
    "timing_first_action_seconds": (
        "Tempo decorrido até a primeira ação do respondente no item",
        "Tiempo transcurrido hasta la primera acción del encuestado en el ítem",
    ),
    "n_actions": (
        "Número de ações do respondente no item",
        "Número de acciones del encuestado en el ítem",
    ),
    "n_visits": (
        "Número de vezes que o respondente visitou o item",
        "Número de veces que el encuestado visitó el ítem",
    ),
    "n_short_visits": (
        "Número de visitas ao item mais curtas que a duração mínima do PIAAC",
        "Número de visitas al ítem más cortas que la duración mínima de PIAAC",
    ),
    "variable_name": (
        "Nome da variável no PIAAC, em minúsculas",
        "Nombre de la variable en PIAAC, en minúsculas",
    ),
    "table_id": (
        "Tabela à qual a coluna pertence",
        "Tabla a la que pertenece la columna",
    ),
    "column_name": ("Nome da coluna", "Nombre de la columna"),
    "label": (
        "Rótulo da variável conforme publicado no codebook internacional",
        "Etiqueta de la variable según el codebook internacional",
    ),
    "level": (
        "Nível de medida atribuído pelo codebook",
        "Nivel de medida asignado por el codebook",
    ),
    "bigquery_type": (
        "Tipo do BigQuery com que a variável foi carregada",
        "Tipo de BigQuery con el que se cargó la variable",
    ),
    "measurement_unit": (
        "Unidade de medida; vazio quando a variável não é uma quantidade",
        "Unidad de medida; vacío cuando la variable no es una cantidad",
    ),
    "measure": (
        "Qual medida por item ela registra",
        "Qué medida por ítem registra",
    ),
    "key": (
        "Valor codificado presente na tabela de dados",
        "Valor codificado presente en la tabla de datos",
    ),
    "value": (
        "Significado do valor codificado",
        "Significado del valor codificado",
    ),
    "temporal_coverage": (
        "Anos aos quais o mapeamento se aplica",
        "Años a los que se aplica el mapeo",
    ),
}


def read_architecture(slug: str) -> list[dict]:
    with (CODE_DIR / "architecture" / f"{slug}.csv").open(
        encoding="utf-8"
    ) as handle:
        return list(csv.DictReader(handle))


def columns_payload(slug: str) -> str:
    rows = []
    for column in read_architecture(slug):
        english = column["description"]
        pt, es = TRANSLATED.get(column["name"], (english, english))
        rows.append(
            {
                "name": column["name"],
                "description_pt": pt,
                "description_en": english,
                "description_es": es,
                "bigquery_type": column["bigquery_type"],
                "covered_by_dictionary": column["covered_by_dictionary"],
                "directory_column": column["directory_column"],
                "measurement_unit": column["measurement_unit"],
                "has_sensitive_data": column["has_sensitive_data"],
                "observations": column["observations"],
                "is_partition": column["name"] == "year",
            }
        )
    return json.dumps(rows, ensure_ascii=False)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="staging")
    parser.add_argument("--publish", action="store_true")
    args = parser.parse_args()
    env = args.env
    bucket = "basedosdados" if env == "prod" else "basedosdados-dev"
    gcp_project = "basedosdados" if env == "prod" else "basedosdados-dev"

    tool(BD.auth)(env=env)
    ids = tool(BD.discover_ids)(
        env=env,
        keys=[
            "status",
            "license",
            "availability",
            "organization",
            "theme",
            "entity",
        ],
    )
    account = tool(BD.get_authenticated_account)(env=env)
    account_id = account["id"]
    print(f"authenticated as {account.get('email', account_id)}")

    # create_update_dataset matches on id, not slug: calling it without one
    # creates a second dataset with the same slug rather than updating the first.
    existing = tool(BD.get_dataset)(slug=DATASET_SLUG, env=env)
    existing_id = existing.get("id") if isinstance(existing, dict) else None
    if existing_id:
        print(f"reusing existing dataset {existing_id}")

    dataset = tool(BD.create_update_dataset)(
        id=existing_id,
        slug=DATASET_SLUG,
        name_pt=DATASET_NAME["pt"],
        name_en=DATASET_NAME["en"],
        name_es=DATASET_NAME["es"],
        description_pt=DATASET_DESCRIPTION["pt"],
        description_en=DATASET_DESCRIPTION["en"],
        description_es=DATASET_DESCRIPTION["es"],
        organization_ids=[ids["organization"]["oecd"]],
        theme_ids=[ids["theme"]["education"], ids["theme"]["economics"]],
        tag_ids=[],
        status_id=ids["status"]["under_review"],
        env=env,
    )
    dataset_id = dataset["id"]
    print(f"dataset {DATASET_SLUG}: {dataset_id}")

    source_ids = {}
    for slug, names, url, descriptions in RAW_SOURCES:
        source = tool(BD.create_update_raw_data_source)(
            name_pt=names[0],
            name_en=names[1],
            name_es=names[2],
            description_pt=descriptions[0],
            description_en=descriptions[1],
            description_es=descriptions[2],
            url=url,
            dataset_id=dataset_id,
            availability_id=ids["availability"]["online"],
            license_id=ids["license"]["cc_by_igo"],
            status_id=ids["status"]["published"],
            has_structured_data=True,
            contains_api=False,
            is_free=True,
            requires_registration=False,
            env=env,
        )
        source_ids[slug] = source["id"]
        print(f"  raw source {slug}: {source['id']}")

    for table_slug in TABLE_NAMES:
        names = TABLE_NAMES[table_slug]
        table = tool(BD.create_update_table)(
            slug=table_slug,
            name_pt=names[0],
            name_en=names[1],
            name_es=names[2],
            description_pt=arch.TABLE_DESCRIPTIONS[table_slug],
            description_en=arch.TABLE_DESCRIPTIONS[table_slug],
            description_es=arch.TABLE_DESCRIPTIONS[table_slug],
            dataset_id=dataset_id,
            status_id=ids["status"]["published"],
            published_by_ids=[account_id],
            data_cleaned_by_ids=[account_id],
            auxiliary_files_url=BUCKET_URL.format(
                bucket=bucket, ds=GCP_DATASET, table=table_slug
            ),
            raw_data_source_ids=[source_ids[TABLE_SOURCE[table_slug]]],
            env=env,
        )
        table_id = table["id"]
        print(f"  table {table_slug}: {table_id}")

        result = tool(BD.bulk_upsert_columns)(
            table_id=table_id,
            columns_json=columns_payload(table_slug),
            env=env,
        )
        print(
            f"    columns: created={len(result.get('created', []))} updated={len(result.get('updated', []))} errors={len(result.get('errors', []))}"
        )
        for error in result.get("errors", [])[:3]:
            print(f"      ERROR {error}")

        tool(BD.create_update_cloud_table)(
            table_id=table_id,
            gcp_project_id=gcp_project,
            gcp_dataset_id=GCP_DATASET,
            gcp_table_id=table_slug,
            env=env,
        )

    print(
        "\nregistered. run with --publish to flip the dataset to published on this env."
    )
    if args.publish:
        tool(BD.create_update_dataset)(
            id=dataset_id,
            slug=DATASET_SLUG,
            name_pt=DATASET_NAME["pt"],
            name_en=DATASET_NAME["en"],
            name_es=DATASET_NAME["es"],
            description_pt=DATASET_DESCRIPTION["pt"],
            description_en=DATASET_DESCRIPTION["en"],
            description_es=DATASET_DESCRIPTION["es"],
            organization_ids=[ids["organization"]["oecd"]],
            theme_ids=[ids["theme"]["education"], ids["theme"]["economics"]],
            tag_ids=[],
            status_id=ids["status"]["published"],
            env=env,
        )
        print("dataset published on", env)


if __name__ == "__main__":
    main()
