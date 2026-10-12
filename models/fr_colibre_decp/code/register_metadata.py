"""Register fr_colibre_decp metadata in the Data Basis backend.

Usage (from the repo root, with the databasis-mcp interpreter):
    PYTHONPATH=. ~/.venvs/databasis-mcp/bin/python \
        -m models.fr_colibre_decp.code.register_metadata \
        [--env staging|dev|prod] [--publish]

Idempotent: every record is looked up first and its id passed back on update,
because create_update_* duplicates observation levels, cloud tables, coverages
and updates when called without one.

Column specs come from the architecture CSVs. English and Spanish descriptions
come from architecture_spec.py, observations from translations.py; a column with
no translation fails the run instead of registering in Portuguese only.

Coverage follows the flow's tier: every table is PartBdpro (the source refreshes
more often than monthly), so each gets a free Coverage (is_closed=False) and a
BD Pro Coverage (is_closed=True) and NO datetime range. The flow's
register_table_materialization_task writes both rolling ranges on its first
prod run; a static range here would declare the paid window free until then.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any, cast

import databasis_mcp.tools.metadata as bd_meta
import databasis_mcp.tools.write as bd_write

from models.fr_colibre_decp.code import translations as tr
from models.fr_colibre_decp.code.architecture_spec import TABLES as SPEC

ARCH = Path(__file__).parent / "architecture"
DATASET_SLUG = "decp"
GCP_DATASET_ID = "fr_colibre_decp"
TABLE_ORDER = ["marche", "modification", "titulaire"]
AREA_SLUG = "fr"
THEME_SLUGS = ["economics", "government"]

# Tag vocabularies are slugged in Portuguese on staging and English on prod.
TAG_SLUGS = {
    "staging": [
        "licitacao",
        "contrato",
        "empresa",
        "administracao_publica",
        "transparencia",
    ],
    "dev": [
        "licitacao",
        "contrato",
        "empresa",
        "administracao_publica",
        "transparencia",
    ],
    "prod": [
        "public_procurement",
        "contract",
        "company",
        "public_administration",
        "spending",
        "transparency",
    ],
}

ORGANIZATION = {
    "slug": "colibre",
    "name_pt": "Colibre",
    "name_en": "Colibre",
    "name_es": "Colibre",
    "description_pt": (
        "Projeto independente de Colin Maudry (colibre.fr, decp.info) que "
        "consolida e publica diariamente os Dados Essenciais da Contratação "
        "Pública (DECP) da França a partir de dezenas de fontes de dados abertos."
    ),
    "description_en": (
        "Independent project by Colin Maudry (colibre.fr, decp.info) that "
        "consolidates and publishes France's Essential Public Procurement Data "
        "(DECP) daily from dozens of open-data sources."
    ),
    "description_es": (
        "Proyecto independiente de Colin Maudry (colibre.fr, decp.info) que "
        "consolida y publica a diario los Datos Esenciales de la Contratación "
        "Pública (DECP) de Francia a partir de decenas de fuentes de datos abiertos."
    ),
    "website": "https://colibre.fr",
}

DATASET = {
    "name_pt": "Dados Essenciais da Contratação Pública (DECP)",
    "name_en": "Essential Public Procurement Data (DECP)",
    "name_es": "Datos Esenciales de la Contratación Pública (DECP)",
    "description_pt": (
        "Contratos públicos da França declarados pelos compradores nos Dados "
        "Essenciais da Contratação Pública (DECP), consolidados diariamente por "
        "colibre.fr a partir de cerca de 60 fontes de dados abertos. Cobre os "
        "contratos notificados desde 2014, com cobertura ampla a partir de 2019, "
        "e registra comprador, objeto, código CPV, procedimento, valor, duração, "
        "modificações e contratados identificados pelo SIRET. A publicação dos "
        "dados essenciais é obrigatória para contratos a partir de 40 mil euros "
        "desde 2020, e a partir de 25 mil euros antes."
    ),
    "description_en": (
        "French public contracts declared by buyers in the Essential Public "
        "Procurement Data (DECP), consolidated daily by colibre.fr from about 60 "
        "open-data sources. It covers contracts notified since 2014, with broad "
        "coverage from 2019, and records the buyer, purpose, CPV code, procedure, "
        "amount, duration, amendments and awardees identified by SIRET. "
        "Publishing the essential data is mandatory for contracts of 40,000 euros "
        "or more since 2020, and of 25,000 euros or more before."
    ),
    "description_es": (
        "Contratos públicos de Francia declarados por los compradores en los "
        "Datos Esenciales de la Contratación Pública (DECP), consolidados a diario "
        "por colibre.fr a partir de unas 60 fuentes de datos abiertos. Cubre los "
        "contratos notificados desde 2014, con cobertura amplia a partir de 2019, "
        "y registra comprador, objeto, código CPV, procedimiento, monto, duración, "
        "modificaciones y adjudicatarios identificados por SIRET. La publicación "
        "de los datos esenciales es obligatoria para contratos de 40.000 euros o "
        "más desde 2020, y de 25.000 euros o más antes."
    ),
}

RAW_SOURCE = {
    "name_pt": "DECP consolidados em formato tabular (data.gouv.fr)",
    "name_en": "Consolidated DECP in tabular format (data.gouv.fr)",
    "name_es": "DECP consolidados en formato tabular (data.gouv.fr)",
    "description_pt": (
        "Arquivo decp.parquet reconstruído diariamente por Colin Maudry a partir "
        "das publicações de dados essenciais dos perfis de compradores e do "
        "Ministério da Economia. Licença: Licence Ouverte 2.0 (Etalab), "
        "compatível com CC-BY."
    ),
    "description_en": (
        "The decp.parquet file rebuilt daily by Colin Maudry from the essential "
        "data published by buyer profiles and the Ministry of the Economy. "
        "License: Licence Ouverte 2.0 (Etalab), compatible with CC-BY."
    ),
    "description_es": (
        "Archivo decp.parquet reconstruido a diario por Colin Maudry a partir de "
        "las publicaciones de datos esenciales de los perfiles de compradores y "
        "del Ministerio de Economía. Licencia: Licence Ouverte 2.0 (Etalab), "
        "compatible con CC-BY."
    ),
    "url": (
        "https://www.data.gouv.fr/datasets/"
        "donnees-essentielles-de-la-commande-publique-consolidees-format-tabulaire"
    ),
}

TABLE_TEXT = {
    "marche": {
        "name_pt": "Contratos",
        "name_en": "Contracts",
        "name_es": "Contratos",
        "description_pt": (
            "Uma linha por contrato público (id_marche), com o comprador e os "
            "atributos e valores da versão inicial do contrato."
        ),
        "description_en": (
            "One row per public contract (id_marche), with the buyer and the "
            "attributes and amounts of the contract's initial version."
        ),
        "description_es": (
            "Una fila por contrato público (id_marche), con el comprador y los "
            "atributos y montos de la versión inicial del contrato."
        ),
    },
    "modification": {
        "name_pt": "Versões e modificações dos contratos",
        "name_en": "Contract versions and amendments",
        "name_es": "Versiones y modificaciones de los contratos",
        "description_pt": (
            "Uma linha por contrato e versão: a atribuição inicial "
            "(id_modification = 0) e cada modificação posterior, que só pode "
            "alterar o valor, a duração e os contratados."
        ),
        "description_en": (
            "One row per contract and version: the initial award "
            "(id_modification = 0) and each later amendment, which can only "
            "change the amount, the duration and the awardees."
        ),
        "description_es": (
            "Una fila por contrato y versión: la adjudicación inicial "
            "(id_modification = 0) y cada modificación posterior, que solo puede "
            "cambiar el monto, la duración y los adjudicatarios."
        ),
    },
    "titulaire": {
        "name_pt": "Contratados",
        "name_en": "Awardees",
        "name_es": "Adjudicatarios",
        "description_pt": (
            "Uma linha por contrato, versão e contratado. Um contrato pode ter "
            "vários contratados, identificados em geral pelo SIRET."
        ),
        "description_en": (
            "One row per contract, version and awardee. A contract can have "
            "several awardees, usually identified by SIRET."
        ),
        "description_es": (
            "Una fila por contrato, versión y adjudicatario. Un contrato puede "
            "tener varios adjudicatarios, identificados en general por SIRET."
        ),
    },
}

# Observation level entity -> the column that identifies it, per table.
LEVELS = {
    "marche": {"contract": "id_marche"},
    "modification": {"contract": "id_marche", "amendment": "id_modification"},
    "titulaire": {
        "contract": "id_marche",
        "amendment": "id_modification",
        "establishment": "id_titulaire",
    },
}
PARTITION_COLUMNS = {"ano"}

# The source's max coverage month at onboarding (latest initial-notification
# month in marche). The flow's commit_source_update_task moves it afterwards.
SOURCE_MAX_DATE = "2026-10-01T00:00:00+00:00"


def fn(name: str) -> Any:
    """The plain function behind an MCP tool (FastMCP keeps it on `.fn`)."""
    f = getattr(bd_meta, name, None) or getattr(bd_write, name)
    return cast(Any, getattr(f, "fn", f))


def read_architecture(table: str) -> list[dict[str, str]]:
    with (ARCH / f"{table}.csv").open(encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def column_payload(table: str) -> list[dict[str, Any]]:
    """Every column of a table, in all three languages."""
    spec = {c.name: c for c in SPEC[table]}
    out = []
    for r in read_architecture(table):
        c = spec[r["name"]]
        col: dict[str, Any] = {
            "name": r["name"],
            "bigquery_type": r["bigquery_type"],
            "description_pt": r["description"],
            "description_en": c.en,
            "description_es": c.es,
            "covered_by_dictionary": r["covered_by_dictionary"] == "yes",
            "has_sensitive_data": r["has_sensitive_data"] == "yes",
        }
        if r["directory_column"]:
            col["directory_column"] = r["directory_column"]
        if r["measurement_unit"]:
            col["measurement_unit"] = r["measurement_unit"]
        if r["observations"]:
            if r["observations"] not in tr.OBSERVATIONS:
                raise KeyError(
                    f"{table}.{r['name']}: no translation for observations "
                    f"{r['observations']!r}"
                )
            en, es = tr.OBSERVATIONS[r["observations"]]
            col["observations_pt"] = r["observations"]
            col["observations_en"] = en
            col["observations_es"] = es
        out.append(col)
    return out


_TABLE_STATE = """
query($slug: String!) {
  allDataset(slug: $slug) {
    edges { node { tables { edges { node {
      slug
      coverages { edges { node { id isClosed
        datetimeRanges { edges { node { id } } } } } }
      updates { edges { node { id latest } } }
    } } } } }
  }
}
"""


def table_state(env: str) -> dict[str, dict[str, Any]]:
    """Coverages by tier and the Update of every table, read over GraphQL.

    get_dataset returns coverages with no is_closed and in no stable order, so
    the free and BD Pro tiers cannot be told apart there.
    """
    data = bd_meta._gql(_TABLE_STATE, {"slug": DATASET_SLUG}, env=env)
    out: dict[str, dict[str, Any]] = {}
    for edge in data["allDataset"]["edges"]:
        for t in edge["node"]["tables"]["edges"]:
            node = t["node"]
            tiers: dict[bool, str] = {}
            for c in node["coverages"]["edges"]:
                tier = bool(c["node"]["isClosed"])
                if tier in tiers:
                    sys.exit(
                        f"{node['slug']}: two Coverages with is_closed={tier}; "
                        "resolve by hand before re-running"
                    )
                tiers[tier] = bd_meta._strip_id(c["node"]["id"])
            updates = [u["node"] for u in node["updates"]["edges"]]
            out[node["slug"]] = {
                "coverages": tiers,
                "update": (
                    {
                        "id": bd_meta._strip_id(updates[0]["id"]),
                        "latest": updates[0]["latest"],
                    }
                    if updates
                    else None
                ),
            }
    return out


def source_update_id(source_id: str, env: str) -> str | None:
    """Id of the raw data source's existing Update, if any."""
    q = """
    query($id: ID!) { allRawdatasource(id: $id) { edges { node {
      updates { edges { node { id } } } } } } }
    """
    data = bd_meta._gql(q, {"id": source_id}, env=env)
    for edge in data["allRawdatasource"]["edges"]:
        updates = edge["node"]["updates"]["edges"]
        if updates:
            return bd_meta._strip_id(updates[0]["node"]["id"])
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default="staging")
    ap.add_argument(
        "--publish",
        action="store_true",
        help=(
            "flip the dataset to published. Safe on dev/staging; on prod only "
            "after the PR merged, table-approve built the tables and they are "
            "verified"
        ),
    )
    args = ap.parse_args()
    env = args.env
    gcp_project = "basedosdados" if env == "prod" else "basedosdados-dev"

    # Fail before writing anything if a column lacks a translation.
    payloads = {t: column_payload(t) for t in TABLE_ORDER}

    ids = fn("discover_ids")(
        env=env, keys=["status", "entity", "license", "availability", "theme"]
    )
    status_review = ids["status"]["under_review"]
    status_published = ids["status"]["published"]
    theme_ids = [ids["theme"][t] for t in THEME_SLUGS]
    tag_ids = []
    for slug in TAG_SLUGS[env]:
        try:
            tag_ids.append(
                fn("lookup_id")(category="tag", slug=slug, env=env)["id"]
            )
        except Exception:
            sys.exit(f"tag {slug!r} not found on {env}")
    area_id = fn("lookup_id")(category="area", slug=AREA_SLUG, env=env)["id"]
    language_fr = fn("lookup_id")(category="language", slug="fr", env=env)[
        "id"
    ]
    account_id = fn("get_authenticated_account")(env=env)["id"]

    try:
        org_id = fn("lookup_id")(
            category="organization", slug=ORGANIZATION["slug"], env=env
        )["id"]
    except Exception:
        org_id = None
    org = fn("create_update_organization")(
        **ORGANIZATION, area_id=area_id, id=org_id, env=env
    )
    print(f"env={env} organization colibre -> {org['id']}")

    existing = fn("get_dataset")(slug=DATASET_SLUG, env=env)
    ds = fn("create_update_dataset")(
        slug=DATASET_SLUG,
        **DATASET,
        organization_ids=[org["id"]],
        theme_ids=theme_ids,
        tag_ids=tag_ids,
        status_id=status_published if args.publish else status_review,
        id=existing.get("id") if existing.get("found") else None,
        env=env,
    )
    dataset_id = ds["id"]
    print(f"dataset {DATASET_SLUG} -> {dataset_id}")

    prior_sources = {
        s["url"]: s["id"]
        for s in fn("get_raw_data_sources")(dataset_slug=DATASET_SLUG, env=env)
        if s.get("url")
    }
    source = fn("create_update_raw_data_source")(
        dataset_id=dataset_id,
        **RAW_SOURCE,
        license_id=ids["license"]["cc_by"],
        availability_id=ids["availability"]["online"],
        has_structured_data=True,
        is_free=True,
        contains_api=False,
        requires_registration=False,
        language_ids=[language_fr],
        status_id=status_published,
        id=prior_sources.get(RAW_SOURCE["url"]),
        env=env,
    )
    source_id = source["id"]
    print(f"  raw source -> {source_id}")

    # Pass an explicit offset: the backend reads a naive timestamp as Sao Paulo
    # time, so a naive UTC value, or a stored value truncated to naive, drifts 3h
    # forward on every run.
    now = dt.datetime.now(dt.UTC).replace(microsecond=0)

    for table in TABLE_ORDER:
        prior = (
            fn("get_dataset")(slug=DATASET_SLUG, env=env)
            .get("tables", {})
            .get(table, {})
        )
        text = TABLE_TEXT[table]
        t = fn("create_update_table")(
            slug=table,
            **text,
            dataset_id=dataset_id,
            status_id=status_published,
            published_by_ids=[account_id],
            data_cleaned_by_ids=[account_id],
            auxiliary_files_url=(
                "https://storage.googleapis.com/basedosdados-public/"
                f"auxiliary_files/{GCP_DATASET_ID}/{table}/auxiliary_files.zip"
            ),
            id=prior.get("id"),
            env=env,
        )
        table_id = t["id"]
        print(f"\ntable {table} -> {table_id}")

        prior_ols = {
            o["entity_slug"]: o["id"]
            for o in prior.get("observation_levels", [])
        }
        ol_ids = {}
        for entity in LEVELS[table]:
            o = fn("create_update_observation_level")(
                table_id=table_id,
                entity_id=ids["entity"][entity],
                id=prior_ols.get(entity),
                env=env,
            )
            ol_ids[entity] = o["id"]
        fn("reorder_observation_levels")(
            table_id=table_id,
            ol_ids=[ol_ids[e] for e in LEVELS[table]],
            env=env,
        )
        print(f"  observation levels: {list(LEVELS[table])}")

        res = fn("bulk_upsert_columns")(
            table_id=table_id,
            columns_json=json.dumps(payloads[table], ensure_ascii=False),
            env=env,
        )
        print(
            f"  columns: created={res['created']} updated={res['updated']} "
            f"errors={res['errors']}"
        )
        if res["errors"]:
            raise RuntimeError(
                f"{table}: column upsert errors {res['errors']}"
            )
        names = [c["name"] for c in payloads[table]]
        fn("reorder_columns")(table_id=table_id, column_names=names, env=env)

        # update_column's booleans default to False and its text fields can
        # overwrite stored ones, so re-pass the whole column with the flag.
        stored = {
            c["name"]: c["id"]
            for c in fn("get_dataset")(slug=DATASET_SLUG, env=env)["tables"][
                table
            ]["columns"]
        }
        by_name = {c["name"]: c for c in payloads[table]}
        linked = {col: ent for ent, col in LEVELS[table].items()}
        for name in sorted(PARTITION_COLUMNS | set(linked)):
            c = by_name[name]
            fn("update_column")(
                column_id=stored[name],
                column_name=name,
                table_id=table_id,
                description_pt=c["description_pt"],
                description_en=c["description_en"],
                description_es=c["description_es"],
                observations_pt=c.get("observations_pt", ""),
                observations_en=c.get("observations_en", ""),
                observations_es=c.get("observations_es", ""),
                measurement_unit=c.get("measurement_unit", ""),
                directory_column_name=c.get("directory_column", ""),
                covered_by_dictionary=c["covered_by_dictionary"],
                has_sensitive_data=c["has_sensitive_data"],
                is_partition=name in PARTITION_COLUMNS,
                observation_level_id=(
                    ol_ids[linked[name]] if name in linked else None
                ),
                env=env,
            )
        print(
            f"  partition {sorted(PARTITION_COLUMNS)}, linked {sorted(linked)}"
        )

        prior_ct = prior.get("cloud_tables", [])
        fn("create_update_cloud_table")(
            table_id=table_id,
            gcp_project_id=gcp_project,
            gcp_dataset_id=GCP_DATASET_ID,
            gcp_table_id=table,
            id=prior_ct[0]["id"] if prior_ct else None,
            env=env,
        )

        state = table_state(env).get(table, {"coverages": {}, "update": None})
        for is_closed in (False, True):
            fn("create_update_coverage")(
                table_id=table_id,
                area_id=area_id,
                is_closed=is_closed,
                id=state["coverages"].get(is_closed),
                env=env,
            )
        print("  coverages: free + BD Pro, ranges left to the flow")

        # Table Update: when we last refreshed. Carry a stored value forward so
        # a metadata-only re-run does not report a refresh that never happened.
        update = state["update"]
        latest = now
        if update:
            stored = dt.datetime.fromisoformat(update["latest"])
            latest = min(stored, now)
        fn("create_update_update")(
            table_id=table_id,
            entity_id=ids["entity"]["week"],
            frequency=1,
            latest=latest.isoformat(),
            id=update["id"] if update else None,
            env=env,
        )

        fn("create_update_table")(
            slug=table,
            **text,
            dataset_id=dataset_id,
            status_id=status_published,
            published_by_ids=[account_id],
            data_cleaned_by_ids=[account_id],
            raw_data_source_ids=[source_id],
            id=table_id,
            env=env,
        )
        print("  cloud table, update and raw source linked")

    # Source Update: what the source has published, as its max coverage month.
    # The flow's commit_source_update_task moves it on every prod run.
    prior_update = source_update_id(source_id, env)
    fn("create_update_update")(
        raw_data_source_id=source_id,
        entity_id=ids["entity"]["month"],
        frequency=1,
        latest=SOURCE_MAX_DATE,
        id=prior_update,
        env=env,
    )
    print(f"source update ({'updated' if prior_update else 'created'})")

    fn("reorder_tables")(
        dataset_slug=DATASET_SLUG, table_slugs=TABLE_ORDER, env=env
    )
    print(f"\n=== METADATA REGISTRATION COMPLETE (env={env}) ===")


if __name__ == "__main__":
    main()
