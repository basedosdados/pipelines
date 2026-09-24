#!/usr/bin/env python3
"""Register br_ibama_fiscalizacao metadata in the Data Basis backend.

Usage::

    python register_metadata.py                 # staging (default)
    python register_metadata.py --env prod      # only after explicit approval

Idempotent by construction: every create_update_* call is given the id of the
record it is updating when that record already exists, because create_update_*
otherwise creates a duplicate rather than updating (see
reference_databasis_create_update_not_idempotent).
"""

from __future__ import annotations

import csv
import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(
    0, "/Users/rdahis/Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp"
)
sys.path.insert(0, str(Path(__file__).parent))

import server
import translations as tr
from constants import DATASET_ID

# create_update_update.latest is a DateTime, not a Date.
NOW = datetime.now().replace(microsecond=0).isoformat()

ENV = "prod" if "--env" in sys.argv and "prod" in sys.argv else "staging"
GCP_PROJECT = "basedosdados" if ENV == "prod" else "basedosdados-dev"
SLUG = "fiscalizacao"  # bare slug; the org prefix is added by the backend
ARCH = Path(__file__).parent / "architecture"

TABLES = ["auto_infracao", "area_embargada", "dicionario"]

# Observation levels per table: entity slug -> the column(s) identifying it.
OBSERVATION_LEVELS = {
    "auto_infracao": {
        "year": ["ano"],
        "municipality": ["id_municipio"],
        "act": ["id_auto"],
    },
    "area_embargada": {
        "year": ["ano"],
        "municipality": ["id_municipio"],
        "act": ["id_embargo"],
    },
    "dicionario": {},
}

COVERAGE = {
    "auto_infracao": (1977, 2026),
    "area_embargada": (1987, 2026),
    "dicionario": (1977, 2026),
}

TAGS = [
    "3ee4d3c4-0ee2-436b-bd7f-76293cbc0bf2",  # fiscalizacao
    "146a0144-71ef-4361-ab2a-8067000e22b6",  # infracao
    "b9c6eff2-eeb8-4dde-b8f1-115706ec7b69",  # multa
    "6039e044-9c41-41af-aabb-ca3d96578f67",  # embargo
    "c84df939-70a7-472e-9b45-eb3ddfa59064",  # desmatamento
    "f21a29fa-c06a-4185-af13-6ead88ef0825",  # sancao
    "37e4fb9e-1984-4b99-b2d6-414bbed49665",  # flora
    "dee3eca4-fec3-44f6-abe3-672d4d9b6431",  # fauna
]

BLOB = (
    "https://stibamadadosabertosprd.blob.core.windows.net/dados-abertos/dados"
)
RAW_SOURCES = {
    "auto_infracao": {
        "name_pt": "Fiscalização - auto de infração",
        "name_en": "Enforcement - infraction notice",
        "name_es": "Fiscalización - acta de infracción",
        "url": "https://dadosabertos.ibama.gov.br/dataset/fiscalizacao-auto-de-infracao",
        "description_pt": (
            "Autos de infração ambiental lavrados pelo Ibama, publicados diariamente "
            "no portal de dados abertos a partir do sistema Sifisc."
        ),
    },
    "area_embargada": {
        "name_pt": "Fiscalização - termo de embargo",
        "name_en": "Enforcement - embargo order",
        "name_es": "Fiscalización - acta de embargo",
        "url": "https://dadosabertos.ibama.gov.br/dataset/fiscalizacao-termo-de-embargo",
        "description_pt": (
            "Termos de embargo lavrados pelo Ibama, publicados diariamente no portal "
            "de dados abertos. O arquivo principal está em "
            f"{BLOB}/TERMOS_DE_EMBARGO/TERMO_EMBARGO/termo_de_embargo.csv; a URL "
            "registrada no CKAN aponta para um caminho que retorna 404."
        ),
    },
    # Secondary source for auto_infracao: situacao_debito and moeda come from here.
    # Deliberately NOT linked to a table — client._raw_source_id raises when a table
    # has two raw sources, which breaks any future recurring pipeline.
    "multas_bens_tutelados": {
        "name_pt": "Multas ambientais distribuídas por bens tutelados",
        "name_en": "Environmental fines by protected asset",
        "name_es": "Multas ambientales por bien tutelado",
        "url": (
            "https://dadosabertos.ibama.gov.br/dataset/"
            "multas-ambientais-distribuidas-por-bens-tutelados"
        ),
        "description_pt": (
            "Série Sicafi com a situação do débito e a moeda de cada auto de infração, "
            "publicada por unidade da federação. Fonte de situacao_debito e moeda em "
            "auto_infracao. Não vinculada a uma tabela porque o cliente do backend "
            "falha quando uma tabela tem mais de uma fonte bruta."
        ),
    },
}

IDS = {}


def log(msg: str) -> None:
    print(msg, flush=True)


def ids(*keys):
    return server.discover_ids(env=ENV, keys=list(keys))


def arch_rows(table: str):
    return list(csv.DictReader(open(ARCH / f"{table}.csv", encoding="utf-8")))


def columns_payload(table: str):
    """Architecture rows -> the bulk_upsert_columns JSON shape, trilingual."""
    out = []
    for order, r in enumerate(arch_rows(table)):
        en, es = tr.COLUMNS[r["description"]]
        out.append(
            {
                "name": r["name"],
                "bigquery_type": r["bigquery_type"],
                "description": r["description"],
                "description_en": en,
                "description_es": es,
                "covered_by_dictionary": r["covered_by_dictionary"] == "yes",
                "directory_column": r["directory_column"] or None,
                "measurement_unit": r["measurement_unit"] or None,
                "has_sensitive_data": r["has_sensitive_data"] == "yes",
                "observations": r["observations"] or None,
                "is_partition": r["name"] == "ano" and table != "dicionario",
                "order": order,
            }
        )
    return out


def main() -> None:
    log(f"=== registering {DATASET_ID} in env={ENV} (gcp {GCP_PROJECT}) ===")
    ref = ids(
        "status", "organization", "theme", "entity", "license", "availability"
    )
    account = server.get_authenticated_account(env=ENV)
    account_id = (
        json.loads(account)["id"]
        if isinstance(account, str)
        else account["id"]
    )
    log(f"  authenticated as account {account_id}")

    existing = server.get_dataset(slug=SLUG, env=ENV)
    existing = json.loads(existing) if isinstance(existing, str) else existing
    dataset_id = existing.get("id") if existing.get("found") else None
    log(f"  existing dataset: {dataset_id or 'none, will create'}")

    res = server.create_update_dataset(
        id=dataset_id,
        slug=SLUG,
        name_pt=tr.DATASET_NAME[0],
        name_en=tr.DATASET_NAME[1],
        name_es=tr.DATASET_NAME[2],
        description_pt=tr.DATASET_DESCRIPTION[0],
        description_en=tr.DATASET_DESCRIPTION[1],
        description_es=tr.DATASET_DESCRIPTION[2],
        organization_ids=[ref["organization"]["ibama"]],
        theme_ids=[ref["theme"]["environment"]],
        tag_ids=TAGS,
        status_id=ref["status"]["under_review"],
        env=ENV,
    )
    dataset_id = res["id"]
    log(f"  dataset -> {dataset_id}")

    # --- raw data sources -------------------------------------------------
    existing_sources = server.get_raw_data_sources(dataset_slug=SLUG, env=ENV)
    if isinstance(existing_sources, str):
        existing_sources = json.loads(existing_sources)
    by_name = {s.get("name"): s.get("id") for s in (existing_sources or [])}
    source_ids = {}
    for key, spec in RAW_SOURCES.items():
        out = server.create_update_raw_data_source(
            id=by_name.get(spec["name_pt"]),
            dataset_id=dataset_id,
            name_pt=spec["name_pt"],
            name_en=spec["name_en"],
            name_es=spec["name_es"],
            description_pt=spec["description_pt"],
            url=spec["url"],
            availability_id=ref["availability"]["online"],
            license_id=ref["license"]["unknown"],
            env=ENV,
        )
        source_ids[key] = out["id"]
        log(f"  raw source {key} -> {out['id']}")

    # --- tables -----------------------------------------------------------
    snapshot = server.get_dataset(slug=SLUG, env=ENV)
    snapshot = json.loads(snapshot) if isinstance(snapshot, str) else snapshot
    prior = snapshot.get("tables", {}) or {}

    for table in TABLES:
        log(f"\n--- {table}")
        names = tr.TABLE_NAMES[table]
        descs = tr.TABLE_DESCRIPTIONS[table]
        was = prior.get(table, {})
        t = server.create_update_table(
            id=was.get("id"),
            slug=table,
            dataset_id=dataset_id,
            name_pt=names[0],
            name_en=names[1],
            name_es=names[2],
            description_pt=descs[0],
            description_en=descs[1],
            description_es=descs[2],
            status_id=ref["status"]["published"],
            published_by_ids=[account_id],
            data_cleaned_by_ids=[account_id],
            env=ENV,
        )
        table_id = t["id"]
        log(f"    table -> {table_id}")

        # observation levels
        ol_ids = {}
        prior_ol = {
            o["entity_slug"]: o["id"]
            for o in (was.get("observation_levels") or [])
        }
        for entity_slug in OBSERVATION_LEVELS[table]:
            ol = server.create_update_observation_level(
                id=prior_ol.get(entity_slug),
                table_id=table_id,
                entity_id=ref["entity"][entity_slug],
                env=ENV,
            )
            ol_ids[entity_slug] = ol["id"]
            log(f"    observation level {entity_slug} -> {ol['id']}")

        # columns
        payload = columns_payload(table)
        server.bulk_upsert_columns(
            table_id=table_id,
            columns_json=json.dumps(payload, ensure_ascii=False),
            env=ENV,
        )
        log(f"    columns -> {len(payload)} upserted")

        # Link each identifying column to its observation level. bulk_upsert_columns
        # does not do this, and without it the site renders the level's columns as
        # "Nao informado". update_column's booleans default to False, so is_partition
        # has to be re-passed on `ano` in the same call.
        cols = server.get_dataset(slug=SLUG, env=ENV)
        cols = json.loads(cols) if isinstance(cols, str) else cols
        col_ids = {
            c["name"]: c["id"] for c in cols["tables"][table]["columns"]
        }
        for entity_slug, col_names in OBSERVATION_LEVELS[table].items():
            for col_name in col_names:
                if col_name not in col_ids:
                    log(
                        f"    WARNING: column {col_name} absent, cannot link OL"
                    )
                    continue
                server.update_column(
                    column_id=col_ids[col_name],
                    column_name=col_name,
                    table_id=table_id,
                    observation_level_id=ol_ids[entity_slug],
                    is_partition=(col_name == "ano"),
                    env=ENV,
                )
                log(
                    f"    linked {col_name} -> observation level {entity_slug}"
                )

        # cloud table
        prior_cloud = (was.get("cloud_tables") or [{}])[0].get("id")
        server.create_update_cloud_table(
            id=prior_cloud,
            table_id=table_id,
            gcp_project_id=GCP_PROJECT,
            gcp_dataset_id=DATASET_ID,
            gcp_table_id=table,
            env=ENV,
        )
        log(f"    cloud table -> {GCP_PROJECT}.{DATASET_ID}.{table}")

        # coverage + temporal range
        area_id = server.lookup_id(slug="br", category="area", env=ENV)
        area_id = (
            area_id["id"]
            if isinstance(area_id, dict)
            else json.loads(area_id)["id"]
        )
        prior_cov = (was.get("coverages") or [{}])[0]
        cov = server.create_update_coverage(
            id=prior_cov.get("id"), table_id=table_id, area_id=area_id, env=ENV
        )
        start, end = COVERAGE[table]
        prior_range = ((prior_cov.get("datetime_ranges") or [{}])[0]).get("id")
        server.create_update_datetime_range(
            id=prior_range,
            coverage_id=cov["id"],
            start_year=start,
            end_year=end,
            interval=1,
            env=ENV,
        )
        log(f"    coverage {start}-{end} -> {cov['id']}")

        # Table-anchored Update: `latest` is when WE last refreshed the table,
        # not the max date in the data. The source republishes daily.
        prior_update = (was.get("updates") or [{}])[0].get("id")
        server.create_update_update(
            id=prior_update,
            table_id=table_id,
            entity_id=ref["entity"]["day"],
            frequency=1,
            latest=NOW,
            env=ENV,
        )
        log("    update record -> daily")

        # Deferred raw-source link. create_update_table does no partial updates, so
        # every required field is re-passed or the table's names are blanked.
        if table in source_ids:
            server.create_update_table(
                id=table_id,
                slug=table,
                dataset_id=dataset_id,
                name_pt=names[0],
                name_en=names[1],
                name_es=names[2],
                description_pt=descs[0],
                description_en=descs[1],
                description_es=descs[2],
                status_id=ref["status"]["published"],
                published_by_ids=[account_id],
                data_cleaned_by_ids=[account_id],
                raw_data_source_ids=[source_ids[table]],
                env=ENV,
            )
            log(f"    linked raw source {table}")

    log("\n=== done ===")


if __name__ == "__main__":
    main()
