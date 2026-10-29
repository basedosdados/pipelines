"""Register the us_census_bds metadata in the Data Basis backend.

Idempotent: every record is looked up before it is written and its id passed
back, because create_update_* duplicates a record when called without one.

Table writes come first and coverages last: ``CreateUpdateTable`` fails with
``'TableForm' has no field named 'coverages_areas'`` on a table that already
has a coverage.

Usage:
    ~/.venvs/bd-pipelines/bin/python -m models.us_census_bds.code.metadata --env staging
    ~/.venvs/bd-pipelines/bin/python -m models.us_census_bds.code.metadata --env staging --publish
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json

import databasis_mcp.tools.metadata as bd_mcp_metadata
import databasis_mcp.tools.write as bd_mcp_write

from models.us_census_bds.code.build_dbt import DESCRIPTIONS
from pipelines.datasets.us_census_bds.constants import constants

ARCH = constants.ARCHITECTURE_DIR.value
DATASET_SLUG = "bds"
GCP_DATASET = constants.DATASET_ID.value
SOURCE_URL = constants.BASE_URL.value

NAME = (
    "Estatísticas de Dinâmica Empresarial (BDS)",
    "Business Dynamics Statistics (BDS)",
    "Estadísticas de Dinámica Empresarial (BDS)",
)
DESCRIPTION = (
    "Estatísticas anuais da dinâmica de firmas e estabelecimentos da "
    "economia privada não agrícola dos Estados Unidos desde 1978, produzidas "
    "pelo U.S. Census Bureau a partir do Longitudinal Business Database. "
    "Trazem contagens de firmas, estabelecimentos e emprego, entrada e saída "
    "de estabelecimentos, criação e destruição de empregos e mortes de "
    "firmas, por estado, condado e área metropolitana, setor e indústria "
    "NAICS, idade e tamanho de firma e de estabelecimento, e seus "
    "cruzamentos.",
    "Annual statistics on the dynamics of firms and establishments in the "
    "United States private non-farm economy since 1978, produced by the U.S. "
    "Census Bureau from the Longitudinal Business Database. They carry counts "
    "of firms, establishments and employment, establishment entry and exit, "
    "job creation and destruction, and firm deaths, by state, county and "
    "metropolitan area, NAICS sector and industry, firm and establishment "
    "age and size, and their crosses.",
    "Estadísticas anuales de la dinámica de empresas y establecimientos de "
    "la economía privada no agrícola de los Estados Unidos desde 1978, "
    "producidas por el U.S. Census Bureau a partir del Longitudinal Business "
    "Database. Incluyen recuentos de empresas, establecimientos y empleo, "
    "entrada y salida de establecimientos, creación y destrucción de empleos "
    "y muertes de empresas, por estado, condado y área metropolitana, sector "
    "e industria NAICS, edad y tamaño de empresa y de establecimiento, y sus "
    "cruces.",
)

# Staging still carries the older Portuguese tag slugs; production uses
# English ones. Same tags either way (ids match where both exist).
TAG_SLUGS = {
    "staging": [
        "empresa", "estabelecimento", "emprego", "empreendedorismo",
        "trabalho", "small-business", "labor-turnover",
    ],
    "prod": [
        "firm", "establishment", "employment", "entrepreneurship", "labor",
        "small-business", "labor-turnover",
    ],
}  # fmt: skip
TAG_SLUGS["dev"] = TAG_SLUGS["staging"]
THEME_SLUGS = ["economics"]

SOURCE_NAME = (
    "Tabelas de séries temporais do BDS",
    "BDS time-series tables",
    "Tablas de series temporales del BDS",
)
SOURCE_DESC = (
    "Arquivos CSV publicados pelo U.S. Census Bureau, um por cruzamento de "
    "dimensões, em um diretório por edição anual. Obras do Governo dos "
    "Estados Unidos não estão sujeitas a direitos autorais (17 U.S.C. "
    "s.105) e são de domínio público.",
    "CSV files published by the U.S. Census Bureau, one per cross of "
    "dimensions, in one directory per annual release. Works of the United "
    "States Government are not subject to copyright (17 U.S.C. s.105) and "
    "are in the public domain.",
    "Archivos CSV publicados por el U.S. Census Bureau, uno por cruce de "
    "dimensiones, en un directorio por edición anual. Las obras del "
    "Gobierno de los Estados Unidos no están sujetas a derechos de autor "
    "(17 U.S.C. s.105) y son de dominio público.",
)

TABLE_NAMES = {
    "country": ("País", "Country", "País"),
    "state": ("Estado", "State", "Estado"),
    "county": ("Condado", "County", "Condado"),
    "metropolitan_area": (
        "Área metropolitana",
        "Metropolitan area",
        "Área metropolitana",
    ),
    "dictionary": ("Dicionário", "Dictionary", "Diccionario"),
}
TABLE_ORDER = list(TABLE_NAMES)

# table -> (entity slug, identifying column) for each observation level
OBSERVATION_LEVELS = {
    "country": [("year", "year"), ("industry", "naics_id")],
    "state": [("state", "state_id"), ("year", "year"), ("industry", "naics_id")],
    "county": [("county", "county_id"), ("year", "year"), ("industry", "naics_id")],
    "metropolitan_area": [
        ("metropolitan_area", "cbsa_id"),
        ("year", "year"),
        ("industry", "naics_id"),
    ],
}  # fmt: skip

FIRST_YEAR, LAST_YEAR = 1978, 2023
# BDS year t is released around September of t + 2.
UPDATE_LAG = 2
# What the source has published: its max coverage date, not a wall clock.
SOURCE_LATEST = f"{LAST_YEAR}-01-01T00:00:00+00:00"

AUX_URL = (
    "https://storage.googleapis.com/basedosdados-public/auxiliary_files/"
    f"{GCP_DATASET}/{{table}}/auxiliary_files.zip"
)

COVERAGE_QUERY = """
{ allTable(id: "%s") { edges { node {
    coverages { edges { node { id isClosed
      datetimeRanges { edges { node { id } } } } } }
    updates { edges { node { id } } }
    observationLevels { edges { node { id entity { slug } } } } } } } }
"""


def column_payload(table: str) -> list[dict]:
    """Build the bulk_upsert_columns payload from a table's architecture CSV."""
    with (ARCH / f"{table}.csv").open() as f:
        rows = list(csv.DictReader(f))
    return [
        {
            "name": c["name"],
            "bigquery_type": c["bigquery_type"],
            "description_pt": c["description_pt"],
            "description_en": c["description"],
            "description_es": c["description_es"],
            "temporal_coverage": c["temporal_coverage"],
            "covered_by_dictionary": c["covered_by_dictionary"],
            "directory_column": c["directory_column"],
            "measurement_unit": c["measurement_unit"],
            "has_sensitive_data": c["has_sensitive_data"],
            "observations_pt": c["observations_pt"],
            "observations_en": c["observations"],
            "observations_es": c["observations_es"],
        }
        for c in rows
    ]


def read_state(table_id: str, env: str) -> dict:
    """Read a table's coverages, updates and observation levels by id."""
    node = bd_mcp_metadata._gql(COVERAGE_QUERY % table_id, {}, env=env)[
        "allTable"
    ]["edges"][0]["node"]
    strip = bd_mcp_metadata._strip_id
    return {
        "coverages": [
            {
                "id": strip(e["node"]["id"]),
                "is_closed": e["node"]["isClosed"],
                "ranges": [
                    strip(r["node"]["id"])
                    for r in e["node"]["datetimeRanges"]["edges"]
                ],
            }
            for e in node["coverages"]["edges"]
        ],
        "updates": [strip(u["node"]["id"]) for u in node["updates"]["edges"]],
        "observation_levels": {
            o["node"]["entity"]["slug"]: strip(o["node"]["id"])
            for o in node["observationLevels"]["edges"]
        },
    }


def tag_ids(env: str) -> list[str]:
    return [
        bd_mcp_metadata.lookup_id("tag", slug, env=env)["id"]
        for slug in TAG_SLUGS[env]
    ]


def upsert_dataset(env: str, ids: dict, status: str) -> str:
    existing = bd_mcp_metadata.get_dataset(DATASET_SLUG, env=env)
    org = bd_mcp_metadata.lookup_id("organization", "census_bureau", env=env)
    result = bd_mcp_write.create_update_dataset(
        slug=DATASET_SLUG,
        name_pt=NAME[0],
        name_en=NAME[1],
        name_es=NAME[2],
        description_pt=DESCRIPTION[0],
        description_en=DESCRIPTION[1],
        description_es=DESCRIPTION[2],
        organization_ids=[org["id"]],
        theme_ids=[ids["theme"][t] for t in THEME_SLUGS],
        tag_ids=tag_ids(env),
        status_id=ids["status"][status],
        id=existing.get("id") if existing.get("found") else None,
        env=env,
    )
    return result.get("id") or existing["id"]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--env", default="staging", choices=["dev", "staging", "prod"]
    )
    parser.add_argument("--gcp-project", default="basedosdados-dev")
    parser.add_argument(
        "--publish",
        action="store_true",
        help="Only flip the dataset status to published.",
    )
    args = parser.parse_args()
    env = args.env
    ids = bd_mcp_metadata.discover_ids(
        env=env, keys=["status", "theme", "entity", "license", "availability"]
    )
    if args.publish:
        if env == "prod":
            raise SystemExit("prod is published only after the PR merges")
        print("published:", upsert_dataset(env, ids, "published"))
        return 0

    status = ids["status"]
    entity = ids["entity"]
    account = bd_mcp_metadata.get_authenticated_account(env=env)["id"]
    area = bd_mcp_metadata.lookup_id("area", "us", env=env)["id"]

    dataset_id = upsert_dataset(env, ids, "under_review")
    print(f"dataset {DATASET_SLUG}: {dataset_id}")

    prior = bd_mcp_write.get_raw_data_sources(DATASET_SLUG, env=env)
    if isinstance(prior, dict):
        prior = prior.get("raw_data_sources", [])
    source_prior = next(
        (s["id"] for s in prior if s.get("url") == SOURCE_URL), None
    )
    source = bd_mcp_write.create_update_raw_data_source(
        dataset_id=dataset_id,
        name_pt=SOURCE_NAME[0],
        name_en=SOURCE_NAME[1],
        name_es=SOURCE_NAME[2],
        url=SOURCE_URL,
        license_id=ids["license"]["cc0"],
        availability_id=ids["availability"]["online"],
        description_pt=SOURCE_DESC[0],
        description_en=SOURCE_DESC[1],
        description_es=SOURCE_DESC[2],
        has_structured_data=True,
        is_free=True,
        contains_api=False,
        requires_registration=False,
        id=source_prior,
        env=env,
    )
    source_id = source.get("id") or source_prior
    print(f"raw source: {source_id}")

    print("=== pass 1: tables, cloud tables, columns ===")
    for table in TABLE_ORDER:
        prior_table = (
            bd_mcp_metadata.get_dataset(DATASET_SLUG, env=env)
            .get("tables", {})
            .get(table, {})
        )
        pt, en, es = TABLE_NAMES[table]
        d_en, d_pt, d_es = DESCRIPTIONS[table]
        table_kwargs = dict(
            slug=table,
            name_pt=pt,
            name_en=en,
            name_es=es,
            dataset_id=dataset_id,
            status_id=status["published"],
            published_by_ids=[account],
            data_cleaned_by_ids=[account],
            description_pt=d_pt,
            description_en=d_en,
            description_es=d_es,
            raw_data_source_ids=[source_id],
            auxiliary_files_url=(
                "" if table == "dictionary" else AUX_URL.format(table=table)
            ),
            id=prior_table.get("id"),
            env=env,
        )
        try:
            res = bd_mcp_write.create_update_table(**table_kwargs)
        except Exception as err:
            # The backend refuses table updates once a coverage exists; the
            # table record is then already in place, so carry on.
            if "coverages_areas" not in str(err):
                raise
            print(f"  {table}: table update skipped (has coverage)")
            res = {}
        table_id = res.get("id") or prior_table.get("id")
        bd_mcp_write.create_update_cloud_table(
            table_id=table_id,
            gcp_project_id=args.gcp_project,
            gcp_dataset_id=GCP_DATASET,
            gcp_table_id=table,
            id=(prior_table.get("cloud_tables") or [{}])[0].get("id"),
            env=env,
        )
        payload = column_payload(table)
        result = bd_mcp_write.bulk_upsert_columns(
            table_id=table_id,
            columns_json=json.dumps(payload, ensure_ascii=False),
            env=env,
        )
        errors = result.get("errors") if isinstance(result, dict) else None
        print(
            f"  {table}: {table_id}, {len(payload)} columns, errors={errors}"
        )

    print("=== pass 2: observation levels, coverage, updates ===")
    tables = bd_mcp_metadata.get_dataset(DATASET_SLUG, env=env)["tables"]
    now = dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat()
    for table in TABLE_ORDER:
        if table == "dictionary":
            continue
        info = tables[table]
        table_id = info["id"]
        columns = {c["name"]: c["id"] for c in info["columns"]}
        state = read_state(table_id, env)

        ol_ids = []
        for entity_slug, column_name in OBSERVATION_LEVELS[table]:
            res = bd_mcp_write.create_update_observation_level(
                table_id=table_id,
                entity_id=entity[entity_slug],
                id=state["observation_levels"].get(entity_slug),
                env=env,
            )
            ol_id = res.get("id") or state["observation_levels"][entity_slug]
            ol_ids.append(ol_id)
            # update_column's booleans default to False, so the partition
            # flag is re-passed on year.
            bd_mcp_write.update_column(
                column_id=columns[column_name],
                column_name=column_name,
                table_id=table_id,
                observation_level_id=ol_id,
                is_partition=column_name == "year",
                env=env,
            )
        bd_mcp_write.reorder_observation_levels(
            table_id=table_id, ol_ids=ol_ids, env=env
        )

        free_prior = next(
            (c for c in state["coverages"] if not c["is_closed"]), {}
        )
        coverage_id = bd_mcp_write.create_update_coverage(
            table_id=table_id,
            area_id=area,
            is_closed=False,
            id=free_prior.get("id"),
            env=env,
        ).get("id") or free_prior.get("id")
        bd_mcp_write.create_update_datetime_range(
            coverage_id=coverage_id,
            start_year=FIRST_YEAR,
            end_year=LAST_YEAR,
            interval=1,
            is_closed=False,
            id=(free_prior.get("ranges") or [None])[0],
            env=env,
        )
        bd_mcp_write.create_update_update(
            entity_id=entity["year"],
            frequency=1,
            lag=UPDATE_LAG,
            latest=now,
            table_id=table_id,
            id=(state["updates"] or [None])[0],
            env=env,
        )
        print(
            f"  {table}: {len(ol_ids)} OLs, coverage {FIRST_YEAR}-{LAST_YEAR}"
        )

    query = (
        f'{{ allUpdate(rawDataSource_Id: "{source_id}") '
        "{ edges { node { id } } } }"
    )
    existing = [
        bd_mcp_metadata._strip_id(e["node"]["id"])
        for e in bd_mcp_metadata._gql(query, {}, env=env)["allUpdate"]["edges"]
    ]
    bd_mcp_write.create_update_update(
        entity_id=entity["year"],
        frequency=1,
        latest=SOURCE_LATEST,
        raw_data_source_id=source_id,
        id=existing[0] if existing else None,
        env=env,
    )
    print(f"source update -> {SOURCE_LATEST[:10]}")

    bd_mcp_write.reorder_tables(
        dataset_slug=DATASET_SLUG, table_slugs=TABLE_ORDER, env=env
    )
    print("table order set")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
