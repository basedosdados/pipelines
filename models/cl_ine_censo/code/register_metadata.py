#!/usr/bin/env python3
"""Register cl_ine_censo metadata in the Data Basis backend.

Run with the shared venv interpreter directly (never `uv run`, which re-syncs the
venv under long jobs)::

    ~/.venvs/bd-pipelines/bin/python register_metadata.py --env staging

The databasis MCP tools are plain Python functions, so they are imported and
called in a loop. That matters here: the column payloads reach 100 KB per table
and cannot sensibly be pasted through a tool call one at a time.

IDEMPOTENCE: create_update_observation_level / _cloud_table / _coverage /
_update all DUPLICATE when re-run without an explicit id. Every one of those is
therefore looked up on the existing dataset first and its id passed back.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(
    0, str(Path.home() / "Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp")
)
import server
from constants import CENSUS_YEAR, DATA_ROOT, DATASET_ID

PAYLOAD_DIR = DATA_ROOT / "metadata_payloads"
AUX_URLS = DATA_ROOT / "aux" / "urls.json"

# --- staging reference ids (re-resolve for prod; they differ per environment) --
IDS = {
    "dataset": "3e9628c5-4d5e-490c-b5f1-3c2bf6ab58ce",
    "raw_data_source": "ad2364b8-ec36-4c5d-b6b5-5b1869ec8e79",
    "organization": "c19aaea7-e3c2-412d-a40d-404f91ce7081",
    "status_published": "e16221de-ac30-4926-83d3-de219998dab3",
    "account": "57",
    "area_cl": "b08e39e7-966c-4d33-ae58-b513c47291d2",
}

ENTITIES = {
    "year": "e1bf146e-b6bb-4b65-bee7-c800876e80a5",
    "municipality": "460cf58b-63a7-4fb7-910f-4ca8ea58c25e",
    "household": "d109791b-0e3c-402d-bcd5-8fec218ce95d",
    "household_group": "4fd592a0-1498-420b-851c-146333783944",
    "person": "b4e76213-888b-40ea-b877-d82ce76d71a2",
    "census_block": "790ac8a5-4456-4133-a069-ff7a40c79d42",
    "census_tract": "a60054ba-3f9a-4772-818a-e11894404e9c",
    "village": "a8537c04-7fdf-4f7d-82a8-6160c8879578",
}

# Per table: display names, descriptions, and the observation levels with the
# column that identifies each. Linking the column is what stops the site showing
# "Nao informado" against the level.
TABLES: dict[str, dict] = {
    "persona": {
        "names": ("Pessoas", "People", "Personas"),
        "descriptions": (
            "Microdados de pessoas do Censo de Populacao e Vivienda 2024 do Chile, com uma linha por pessoa recenseada. O maior nivel de desagregacao geografica e a comuna: o INE nao publica a manzana nem a zona censitaria de cada pessoa por controle de divulgacao estatistica, de modo que esta tabela NAO se cruza com manzana_entidad nem com zona_localidad.",
            "Person-level microdata from Chile's 2024 Population and Housing Census, one row per person enumerated. The finest geographic level is the commune: INE does not publish each person's block or census zone, for statistical disclosure control, so this table does NOT join to manzana_entidad or zona_localidad.",
            "Microdatos de personas del Censo de Poblacion y Vivienda 2024 de Chile, con una fila por persona censada. El maximo nivel de desagregacion geografica es la comuna: el INE no publica la manzana ni la zona censal de cada persona por control de divulgacion estadistica, de modo que esta tabla NO se puede cruzar con manzana_entidad ni zona_localidad.",
        ),
        "levels": [
            ("year", "ano"),
            ("municipality", "id_comuna"),
            ("household", "id_vivienda"),
            ("household_group", "id_hogar"),
            ("person", "id_persona"),
        ],
    },
    "hogar": {
        "names": ("Lares", "Households", "Hogares"),
        "descriptions": (
            "Microdados de lares do Censo de Populacao e Vivienda 2024 do Chile, com uma linha por lar recenseado. Liga-se a vivienda por id_vivienda e a persona por id_vivienda e id_hogar.",
            "Household-level microdata from Chile's 2024 Population and Housing Census, one row per household enumerated. Links to vivienda by id_vivienda and to persona by id_vivienda and id_hogar.",
            "Microdatos de hogares del Censo de Poblacion y Vivienda 2024 de Chile, con una fila por hogar censado. Se vincula con vivienda por id_vivienda y con persona por id_vivienda e id_hogar.",
        ),
        "levels": [
            ("year", "ano"),
            ("municipality", "id_comuna"),
            ("household", "id_vivienda"),
            ("household_group", "id_hogar"),
        ],
    },
    "vivienda": {
        "names": ("Domicilios", "Dwellings", "Viviendas"),
        "descriptions": (
            "Microdados de domicilios do Censo de Populacao e Vivienda 2024 do Chile, com uma linha por domicilio recenseado. O total publicado pelo INE (7.642.716) e menor que o numero de linhas porque exclui os operativos de vivienda colectiva, que aqui aparecem como registros com as perguntas de domicilio nulas.",
            "Dwelling-level microdata from Chile's 2024 Population and Housing Census, one row per dwelling enumerated. INE's published total (7,642,716) is lower than the row count because it excludes the collective-dwelling operations, which appear here as records with the dwelling questions left null.",
            "Microdatos de viviendas del Censo de Poblacion y Vivienda 2024 de Chile, con una fila por vivienda censada. El total publicado por el INE (7.642.716) es menor que el numero de filas porque excluye los operativos de vivienda colectiva, que aqui aparecen como registros con las preguntas de vivienda en nulo.",
        ),
        "levels": [
            ("year", "ano"),
            ("municipality", "id_comuna"),
            ("household", "id_vivienda"),
        ],
    },
    "manzana_entidad": {
        "names": ("Manzana-entidade", "Block-entity", "Manzana-entidad"),
        "descriptions": (
            "Base manzana-entidade do Censo 2024 do Chile: 189 variaveis agregadas de populacao, lares e domicilios por manzana urbana ou entidade rural, com a geometria do poligono. Uniao das camadas cartograficas Manzanas e Entidades publicadas pelo INE; a coluna nivel_geografico indica a origem de cada linha. Somar n_per nesta tabela da 18.226.208 pessoas e nao o total censitario de 18.480.432: a cartografia nao tem poligono para os registros contenedores comunais (113.447 pessoas sem geografia no nivel da manzana) nem para as pessoas recenseadas em domicilios coletivos ou em situacao de rua.",
            "Block-entity base of Chile's 2024 Census: 189 aggregated population, household and dwelling variables per urban block or rural entity, with the polygon geometry. Union of INE's Manzanas and Entidades cartographic layers; the nivel_geografico column records each row's origin. Summing n_per over this table gives 18,226,208 people rather than the census total of 18,480,432: the cartography has no polygon for the commune-container records (113,447 people with no block-level geography), nor for people enumerated in collective dwellings or living on the street.",
            "Base manzana-entidad del Censo 2024 de Chile: 189 variables agregadas de poblacion, hogares y viviendas por manzana urbana o entidad rural, con la geometria del poligono. Union de las capas cartograficas Manzanas y Entidades publicadas por el INE; la columna nivel_geografico indica el origen de cada fila. Sumar n_per sobre esta tabla da 18.226.208 personas y no el total censal de 18.480.432: la cartografia no tiene poligono para los registros contenedores comunales (113.447 personas sin geografia a nivel de manzana) ni para las personas censadas en viviendas colectivas o en situacion de calle.",
        ),
        "levels": [
            ("year", "ano"),
            ("municipality", "id_comuna"),
            ("census_block", "manzent"),
        ],
    },
    "zona_localidad": {
        "names": ("Zona-localidade", "Zone-locality", "Zona-localidad"),
        "descriptions": (
            "Base zona-localidade do Censo 2024 do Chile: 189 variaveis agregadas de populacao, lares e domicilios por zona censitaria urbana ou localidade rural, com a geometria do poligono. Uniao das camadas cartograficas Zonal e Localidades publicadas pelo INE; a coluna nivel_geografico indica a origem de cada linha. Somar n_per nesta tabela da 18.226.208 pessoas e nao o total censitario de 18.480.432: a cartografia nao tem poligono para os registros contenedores comunais (113.447 pessoas sem geografia no nivel da manzana) nem para as pessoas recenseadas em domicilios coletivos ou em situacao de rua.",
            "Zone-locality base of Chile's 2024 Census: 189 aggregated population, household and dwelling variables per urban census zone or rural locality, with the polygon geometry. Union of INE's Zonal and Localidades cartographic layers; the nivel_geografico column records each row's origin. Summing n_per over this table gives 18,226,208 people rather than the census total of 18,480,432: the cartography has no polygon for the commune-container records (113,447 people with no block-level geography), nor for people enumerated in collective dwellings or living on the street.",
            "Base zona-localidad del Censo 2024 de Chile: 189 variables agregadas de poblacion, hogares y viviendas por zona censal urbana o localidad rural, con la geometria del poligono. Union de las capas cartograficas Zonal y Localidades publicadas por el INE; la columna nivel_geografico indica el origen de cada fila. Sumar n_per sobre esta tabla da 18.226.208 personas y no el total censal de 18.480.432: la cartografia no tiene poligono para los registros contenedores comunales (113.447 personas sin geografia a nivel de manzana) ni para las personas censadas en viviendas colectivas o en situacion de calle.",
        ),
        "levels": [
            ("year", "ano"),
            ("municipality", "id_comuna"),
            ("census_tract", "id_zona"),
            ("village", "id_localidad"),
        ],
    },
    "dicionario": {
        "names": ("Dicionario", "Dictionary", "Diccionario"),
        "descriptions": (
            "Dicionario das colunas codificadas do Censo 2024 do Chile, com uma linha por combinacao de tabela, coluna e codigo. Inclui os codigos sentinela -99 (nao resposta) e -66 (valor suprimido por anonimizacao). Os rotulos vem do dicionario Redatam oficial CPV2024.dicX.",
            "Dictionary of the coded columns of Chile's 2024 Census, one row per table-column-code combination. Includes the sentinel codes -99 (no answer) and -66 (value suppressed for anonymisation). Labels come from INE's official Redatam dictionary CPV2024.dicX.",
            "Diccionario de las columnas codificadas del Censo 2024 de Chile, con una fila por combinacion de tabla, columna y codigo. Incluye los codigos centinela -99 (no respuesta) y -66 (valor suprimido por anonimizacion). Las etiquetas provienen del diccionario Redatam oficial CPV2024.dicX.",
        ),
        # A dictionary has no observation level: its grain is metadata about
        # other tables, not an entity in the world.
        "levels": [],
    },
}


def auxiliary_urls() -> dict[str, str]:
    """Per-table auxiliary-file URLs written by build_auxiliary_files.py.

    These currently return HTTP 400 to anonymous visitors: the only bucket
    writable from here is requester-pays. The field is still set, because the
    bundle is in the documented location and the fix is one bucket migration,
    not six bespoke hosting decisions.
    """
    if not AUX_URLS.exists():
        print(
            f"  ! no auxiliary URLs at {AUX_URLS}; run build_auxiliary_files.py --upload"
        )
        return {}
    return json.loads(AUX_URLS.read_text("utf-8"))


def existing_state(env: str) -> dict:
    """Read back what already exists, so a re-run updates instead of duplicating."""
    dataset = server.get_dataset(slug=DATASET_ID, env=env)
    if isinstance(dataset, str):
        dataset = json.loads(dataset)
    return dataset if dataset.get("found") else {"tables": {}}


def register(env: str, gcp_project: str) -> None:
    state = existing_state(env)
    tables_state = state.get("tables", {})
    aux = auxiliary_urls()

    for slug, spec in TABLES.items():
        print(f"\n[{slug}]")
        current = tables_state.get(slug, {})
        table_id = current.get("id")

        name_pt, name_en, name_es = spec["names"]
        desc_pt, desc_en, desc_es = spec["descriptions"]

        result = server.create_update_table(
            id=table_id,
            slug=slug,
            dataset_id=IDS["dataset"],
            name_pt=name_pt,
            name_en=name_en,
            name_es=name_es,
            description_pt=desc_pt,
            description_en=desc_en,
            description_es=desc_es,
            status_id=IDS["status_published"],
            published_by_ids=[IDS["account"]],
            data_cleaned_by_ids=[IDS["account"]],
            raw_data_source_ids=[IDS["raw_data_source"]],
            auxiliary_files_url=aux.get(slug),
            env=env,
        )
        if isinstance(result, str):
            result = json.loads(result)
        table_id = result.get("id", table_id)
        print(f"  table {table_id}")

        # --- observation levels -------------------------------------------
        existing_levels = {
            level.get("entity_slug"): level.get("id")
            for level in current.get("observation_levels", [])
        }
        level_ids: dict[str, str] = {}
        for entity_slug, column_name in spec["levels"]:
            level = server.create_update_observation_level(
                id=existing_levels.get(entity_slug),
                table_id=table_id,
                entity_id=ENTITIES[entity_slug],
                env=env,
            )
            if isinstance(level, str):
                level = json.loads(level)
            level_ids[column_name] = level.get("id")
        if spec["levels"]:
            print(f"  {len(level_ids)} observation level(s)")

        # --- columns -------------------------------------------------------
        payload = json.loads((PAYLOAD_DIR / f"{slug}.json").read_text("utf-8"))
        server.bulk_upsert_columns(
            table_id=table_id,
            columns_json=json.dumps(payload, ensure_ascii=False),
            env=env,
        )
        print(f"  {len(payload)} columns")

        # --- link each grain column to its observation level ---------------
        # bulk_upsert_columns does NOT set observationLevel, so each grain
        # column needs its own update_column. Without this the site renders the
        # level's columns as "Nao informado".
        #
        # update_column takes a column_id, which only exists after the upsert,
        # so the ids are read back here. Its boolean args also default to False,
        # so is_partition has to be re-passed or it is silently cleared.
        refreshed = existing_state(env)["tables"].get(slug, {})
        column_ids = {
            column["name"]: column["id"]
            for column in refreshed.get("columns", [])
        }
        linked = 0
        for column_name, level_id in level_ids.items():
            column_id = column_ids.get(column_name)
            if not level_id or not column_id:
                print(f"  ! no column id for {column_name}; level not linked")
                continue
            server.update_column(
                column_id=column_id,
                column_name=column_name,
                table_id=table_id,
                observation_level_id=level_id,
                is_partition=column_name == "ano",
                env=env,
            )
            linked += 1
        if level_ids:
            print(
                f"  linked {linked}/{len(level_ids)} column(s) to their level"
            )

        # --- cloud table ----------------------------------------------------
        cloud = (current.get("cloud_tables") or [{}])[0]
        server.create_update_cloud_table(
            id=cloud.get("id"),
            table_id=table_id,
            gcp_project_id=gcp_project,
            gcp_dataset_id=DATASET_ID,
            gcp_table_id=slug,
            env=env,
        )
        print(f"  cloud table -> {gcp_project}.{DATASET_ID}.{slug}")

        # --- coverage -------------------------------------------------------
        # The dicionario has no temporal dimension, matching the precedent for
        # static code tables in br_bd_diretorios_*.
        if slug == "dicionario":
            continue
        coverage = (current.get("coverages") or [{}])[0]
        cov = server.create_update_coverage(
            id=coverage.get("id"),
            table_id=table_id,
            area_id=IDS["area_cl"],
            env=env,
        )
        if isinstance(cov, str):
            cov = json.loads(cov)
        coverage_id = cov.get("id")
        ranges = coverage.get("datetime_ranges") or [{}]
        server.create_update_datetime_range(
            id=ranges[0].get("id"),
            coverage_id=coverage_id,
            start_year=CENSUS_YEAR,
            end_year=CENSUS_YEAR,
            interval=1,
            env=env,
        )
        print(f"  coverage {CENSUS_YEAR}-{CENSUS_YEAR}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--env", default="staging", choices=["staging", "prod"]
    )
    args = parser.parse_args()
    gcp_project = "basedosdados" if args.env == "prod" else "basedosdados-dev"
    if args.env == "prod":
        raise SystemExit(
            "prod ids differ per environment and must be re-resolved first; "
            "update IDS/ENTITIES before running against prod"
        )
    register(args.env, gcp_project)
    print("\ndone")


if __name__ == "__main__":
    main()
