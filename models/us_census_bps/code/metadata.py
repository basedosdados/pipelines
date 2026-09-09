"""Register the us_census_bps metadata in the Data Basis backend.

Idempotent: every record is looked up before it is written, and the existing
id is passed back, because create_update_* duplicates a record when called
without one.

Usage:
    python models/us_census_bps/code/metadata.py --env staging
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

sys.path.insert(
    0, "/Users/rdahis/Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp"
)
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import server

from pipelines.datasets.us_census_bps.constants import constants

ARCH = Path(__file__).resolve().parent / "architecture"
DATASET_SLUG = "building_permits_survey_bps"
GCP_DATASET = constants.DATASET_ID.value
BASE = constants.BASE_URL.value

NAME_PT = "Pesquisa de Alvarás de Construção (BPS)"
NAME_EN = "Building Permits Survey (BPS)"
NAME_ES = "Encuesta de Permisos de Construcción (BPS)"

DESC_PT = (
    "Contagens mensais e anuais de edificações, unidades habitacionais e "
    "valor de construção de novas residências privadas autorizadas por "
    "alvarás de construção nos Estados Unidos, desde 1980. Os dados são "
    "publicados por jurisdição emissora de alvarás, condado, área "
    "estatística metropolitana e estado, separados por tipo de edificação "
    "(1 unidade, 2 unidades, 3 a 4 unidades e 5 ou mais unidades). As "
    "estimativas incluem imputação para jurisdições que não responderam à "
    "pesquisa, e as tabelas trazem também os valores apenas reportados."
)
DESC_EN = (
    "Monthly and annual counts of buildings, housing units and construction "
    "valuation for new privately-owned residential construction authorized "
    "by building permits in the United States, from 1980. The data are "
    "published by permit-issuing jurisdiction, county, metropolitan "
    "statistical area and state, split by structure type (1 unit, 2 units, "
    "3 to 4 units and 5 or more units). Estimates include imputation for "
    "jurisdictions that did not respond to the survey, and the tables also "
    "carry the reported-only figures."
)
DESC_ES = (
    "Recuentos mensuales y anuales de edificaciones, viviendas y valor de "
    "construcción de nuevas obras residenciales privadas autorizadas por "
    "permisos de construcción en Estados Unidos, desde 1980. Los datos se "
    "publican por jurisdicción emisora de permisos, condado, área "
    "estadística metropolitana y estado, separados por tipo de edificación "
    "(1 unidad, 2 unidades, 3 a 4 unidades y 5 o más unidades). Las "
    "estimaciones incluyen imputación para las jurisdicciones que no "
    "respondieron a la encuesta, y las tablas traen también las cifras "
    "solamente reportadas."
)

TAG_SLUGS = ["housing", "construcao", "real_estate", "regulacao"]
NEW_TAGS = [
    {
        "slug": "building-permit",
        "name_pt": "alvará de construção",
        "name_en": "building permit",
        "name_es": "permiso de construcción",
    }
]
THEME_SLUGS = ["economics", "urbanization"]

# geography level -> (source directory, table slugs it feeds)
SOURCES = {
    "place": (
        "Place/",
        ("permit_place_monthly", "permit_place_annual"),
        "Place-level files",
        "Arquivos por jurisdição emissora de alvarás",
        "Archivos por jurisdicción emisora de permisos",
    ),
    "county": (
        "County/",
        ("permit_county_monthly", "permit_county_annual"),
        "County-level files",
        "Arquivos por condado",
        "Archivos por condado",
    ),
    "cbsa": (
        "CBSA%20(beginning%20Jan%202024)/",
        ("permit_cbsa_monthly", "permit_cbsa_annual"),
        "Core Based Statistical Area files",
        "Arquivos por área estatística baseada em núcleo",
        "Archivos por área estadística basada en núcleo",
    ),
    "msa": (
        "Metro%20(ending%202023)/",
        ("permit_msa_monthly", "permit_msa_annual"),
        "Metropolitan Statistical Area files, through 2023",
        "Arquivos por área estatística metropolitana, até 2023",
        "Archivos por área estadística metropolitana, hasta 2023",
    ),
    "state": (
        "State/",
        ("permit_state_monthly", "permit_state_annual"),
        "State-level files",
        "Arquivos por estado",
        "Archivos por estado",
    ),
}

SOURCE_DESC_EN = (
    "Comma-delimited ASCII files published by the U.S. Census Bureau. Works "
    "of the United States Government are not subject to copyright (17 U.S.C. "
    "s.105) and are in the public domain. Record layouts for every geography "
    "level are published at " + BASE + "Documentation/ and are bundled with "
    "each table's auxiliary files."
)
SOURCE_DESC_PT = (
    "Arquivos ASCII separados por vírgula publicados pelo U.S. Census "
    "Bureau. Obras do Governo dos Estados Unidos não estão sujeitas a "
    "direitos autorais (17 U.S.C. s.105) e são de domínio público. Os "
    "layouts de registro de cada nível geográfico estão em "
    + BASE
    + "Documentation/ e acompanham os arquivos auxiliares de cada tabela."
)
SOURCE_DESC_ES = (
    "Archivos ASCII separados por comas publicados por el U.S. Census "
    "Bureau. Las obras del Gobierno de los Estados Unidos no están sujetas a "
    "derechos de autor (17 U.S.C. s.105) y son de dominio público. Los "
    "diseños de registro de cada nivel geográfico están en "
    + BASE
    + "Documentation/ y acompañan los archivos auxiliares de cada tabla."
)

TABLE_NAMES = {
    "permit_place_monthly": (
        "Alvarás por jurisdição, mensal",
        "Permits by place, monthly",
        "Permisos por jurisdicción, mensual",
    ),
    "permit_place_annual": (
        "Alvarás por jurisdição, anual",
        "Permits by place, annual",
        "Permisos por jurisdicción, anual",
    ),
    "permit_county_monthly": (
        "Alvarás por condado, mensal",
        "Permits by county, monthly",
        "Permisos por condado, mensual",
    ),
    "permit_county_annual": (
        "Alvarás por condado, anual",
        "Permits by county, annual",
        "Permisos por condado, anual",
    ),
    "permit_cbsa_monthly": (
        "Alvarás por área estatística baseada em núcleo, mensal",
        "Permits by Core Based Statistical Area, monthly",
        "Permisos por área estadística basada en núcleo, mensual",
    ),
    "permit_cbsa_annual": (
        "Alvarás por área estatística baseada em núcleo, anual",
        "Permits by Core Based Statistical Area, annual",
        "Permisos por área estadística basada en núcleo, anual",
    ),
    "permit_msa_monthly": (
        "Alvarás por área estatística metropolitana, mensal",
        "Permits by Metropolitan Statistical Area, monthly",
        "Permisos por área estadística metropolitana, mensual",
    ),
    "permit_msa_annual": (
        "Alvarás por área estatística metropolitana, anual",
        "Permits by Metropolitan Statistical Area, annual",
        "Permisos por área estadística metropolitana, anual",
    ),
    "permit_state_monthly": (
        "Alvarás por estado, mensal",
        "Permits by state, monthly",
        "Permisos por estado, mensual",
    ),
    "permit_state_annual": (
        "Alvarás por estado, anual",
        "Permits by state, annual",
        "Permisos por estado, anual",
    ),
    "dicionario": ("Dicionário", "Dictionary", "Diccionario"),
}

# table -> (entity slug, identifying column) for each observation level
OBSERVATION_LEVELS = {
    "permit_place_monthly": [
        ("city", "place_id"),
        ("construction", "structure_type"),
    ],
    "permit_place_annual": [
        ("city", "place_id"),
        ("construction", "structure_type"),
    ],
    "permit_county_monthly": [
        ("county", "county_id"),
        ("construction", "structure_type"),
    ],
    "permit_county_annual": [
        ("county", "county_id"),
        ("construction", "structure_type"),
    ],
    "permit_cbsa_monthly": [
        ("metropolitan_area", "cbsa_id"),
        ("construction", "structure_type"),
    ],
    "permit_cbsa_annual": [
        ("metropolitan_area", "cbsa_id"),
        ("construction", "structure_type"),
    ],
    "permit_msa_monthly": [
        ("metropolitan_area", "msa_cmsa_id"),
        ("construction", "structure_type"),
    ],
    "permit_msa_annual": [
        ("metropolitan_area", "msa_cmsa_id"),
        ("construction", "structure_type"),
    ],
    "permit_state_monthly": [
        ("state", "state_id"),
        ("construction", "structure_type"),
    ],
    "permit_state_annual": [
        ("state", "state_id"),
        ("construction", "structure_type"),
    ],
}

# table -> (start, end, free_end) where each is (year, month) or (year, None).
# free_end is the last period that stays public; None means the whole series
# is public. Data Basis paywalls the trailing window of any table that
# refreshes monthly or more often, with a six-month free lag.
COVERAGE = {
    "permit_place_monthly": ((1988, 1), (2026, 7), (2026, 1)),
    "permit_place_annual": ((1980, None), (2025, None), None),
    "permit_county_monthly": ((2000, 1), (2026, 7), (2026, 1)),
    "permit_county_annual": ((1990, None), (2025, None), None),
    "permit_cbsa_monthly": ((2004, 1), (2026, 7), (2026, 1)),
    "permit_cbsa_annual": ((2003, None), (2025, None), None),
    "permit_msa_monthly": ((1988, 1), (2003, 12), None),
    "permit_msa_annual": ((1980, None), (2002, None), None),
    "permit_state_monthly": ((1988, 1), (2026, 7), (2026, 1)),
    "permit_state_annual": ((1980, None), (2025, None), None),
}

TABLE_ORDER = [
    "permit_place_monthly",
    "permit_place_annual",
    "permit_county_monthly",
    "permit_county_annual",
    "permit_cbsa_monthly",
    "permit_cbsa_annual",
    "permit_msa_monthly",
    "permit_msa_annual",
    "permit_state_monthly",
    "permit_state_annual",
    "dicionario",
]

AUX_URL = (
    "https://storage.googleapis.com/basedosdados-dev/auxiliary_files/"
    f"{GCP_DATASET}/{{table}}/auxiliary_files.zip"
)

# Refresh cadence recorded against each table.
UPDATE = {"monthly": ("month", 1, 1), "annual": ("year", 1, 4)}
TODAY = "2026-09-09T00:00:00+00:00"


def next_period(year: int, month: int | None) -> tuple[int, int | None]:
    """Return the period after the given one, so free and pro never overlap."""
    if month is None:
        return year + 1, None
    return (year + 1, 1) if month == 12 else (year, month + 1)


DICIONARIO_DESC = (
    "Dicionário dos valores codificados usados nas tabelas de "
    "us_census_bps, com uma linha por tabela, coluna e chave."
)


def table_description(table: str) -> str:
    """Return the table description used in both the dbt schema and here."""
    from build_dbt import DESCRIPTIONS

    return DESCRIPTIONS[table]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="staging")
    parser.add_argument("--gcp-project", default="basedosdados-dev")
    args = parser.parse_args()
    env = args.env

    ids = server.discover_ids(
        env=env, keys=["status", "theme", "entity", "license", "availability"]
    )
    status = ids["status"]
    account = server.get_authenticated_account(env=env)["id"]
    org = server.lookup_id("organization", "census_bureau", env=env)["id"]

    tag_ids = []
    for slug in TAG_SLUGS:
        tag_ids.append(server.lookup_id("tag", slug, env=env)["id"])
    for tag in NEW_TAGS:
        try:
            tag_ids.append(server.lookup_id("tag", tag["slug"], env=env)["id"])
        except RuntimeError:
            created = server.create_update_tag(env=env, **tag)
            print(f"created tag {tag['slug']}: {created}")
            tag_ids.append(server.lookup_id("tag", tag["slug"], env=env)["id"])

    existing = server.get_dataset(DATASET_SLUG, env=env)
    dataset_id = existing.get("id") if existing.get("found") else None
    result = server.create_update_dataset(
        slug=DATASET_SLUG,
        name_pt=NAME_PT,
        name_en=NAME_EN,
        name_es=NAME_ES,
        description_pt=DESC_PT,
        description_en=DESC_EN,
        description_es=DESC_ES,
        organization_ids=[org],
        theme_ids=[ids["theme"][t] for t in THEME_SLUGS],
        tag_ids=tag_ids,
        status_id=status["under_review"],
        id=dataset_id,
        env=env,
    )
    dataset_id = result.get("id") or dataset_id
    print(f"dataset {DATASET_SLUG}: {dataset_id}")

    prior_sources = server.get_raw_data_sources(DATASET_SLUG, env=env)
    if isinstance(prior_sources, dict):
        prior_sources = prior_sources.get("raw_data_sources", [])
    existing_sources = {
        s["url"]: s["id"] for s in prior_sources if s.get("url")
    }
    source_ids: dict[str, str | None] = {}
    for level, (path, tables, name_en, name_pt, name_es) in SOURCES.items():
        url = BASE + path
        res = server.create_update_raw_data_source(
            dataset_id=dataset_id,
            name_pt=name_pt,
            name_en=name_en,
            name_es=name_es,
            url=url,
            license_id=ids["license"]["cc0"],
            availability_id=ids["availability"]["online"],
            description_pt=SOURCE_DESC_PT,
            description_en=SOURCE_DESC_EN,
            description_es=SOURCE_DESC_ES,
            has_structured_data=True,
            is_free=True,
            contains_api=False,
            requires_registration=False,
            id=existing_sources.get(url),
            env=env,
        )
        source_ids[level] = res.get("id") or existing_sources.get(url)
        print(f"raw source {level}: {source_ids[level]}")
        for table in tables:
            source_ids[table] = source_ids[level]

    state = server.get_dataset(DATASET_SLUG, env=env).get("tables", {})
    for table in TABLE_ORDER:
        pt, en, es = TABLE_NAMES[table]
        desc = (
            DICIONARIO_DESC
            if table == "dicionario"
            else table_description(table)
        )
        prior = state.get(table, {})
        res = server.create_update_table(
            slug=table,
            name_pt=pt,
            name_en=en,
            name_es=es,
            dataset_id=dataset_id,
            status_id=status["published"],
            published_by_ids=[account],
            data_cleaned_by_ids=[account],
            description_pt=desc,
            description_en=desc,
            description_es=desc,
            auxiliary_files_url=(
                "" if table == "dicionario" else AUX_URL.format(table=table)
            ),
            id=prior.get("id"),
            env=env,
        )
        table_id = res.get("id") or prior.get("id")
        print(f"table {table}: {table_id}")

        server.create_update_cloud_table(
            table_id=table_id,
            gcp_project_id=args.gcp_project,
            gcp_dataset_id=GCP_DATASET,
            gcp_table_id=table,
            id=(prior.get("cloud_tables") or [{}])[0].get("id"),
            env=env,
        )

        columns = list(csv.DictReader((ARCH / f"{table}.csv").open()))
        payload = [
            {
                "name": c["name"],
                "bigquery_type": c["bigquery_type"],
                "description": c["description"],
                "description_en": c["description"],
                "temporal_coverage": c["temporal_coverage"],
                "covered_by_dictionary": c["covered_by_dictionary"],
                "directory_column": c["directory_column"],
                "measurement_unit": c["measurement_unit"],
                "has_sensitive_data": c["has_sensitive_data"],
                "observations": c["observations"],
                "observations_en": c["observations"],
            }
            for c in columns
        ]
        server.bulk_upsert_columns(
            table_id=table_id,
            columns_json=json.dumps(payload, ensure_ascii=False),
            env=env,
        )
        print(f"  columns: {len(payload)}")
    print(
        "\nStage 1 complete (dataset, sources, tables, cloud tables, columns)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
