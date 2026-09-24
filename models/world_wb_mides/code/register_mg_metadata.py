"""Register the 43 MG-only tables in the Data Basis backend.

Run with the shared venv's interpreter, which has `fastmcp` and `requests`:

    ~/.venvs/bd-pipelines/bin/python models/world_wb_mides/code/register_mg_metadata.py --dry-run
    ~/.venvs/bd-pipelines/bin/python models/world_wb_mides/code/register_mg_metadata.py

WHY A SCRIPT AND NOT 250 TOOL CALLS
-----------------------------------
43 tables x (table + columns + observation levels + cloud table + coverage +
datetime range + update) is ~250 backend writes, and the column payloads are
tens of kilobytes each. The MCP tools are plain Python functions, so importing
`server` does the same work with the same credentials without pasting every
argument through a conversation.

IDEMPOTENCY IS NOT FREE HERE
----------------------------
`create_update_*` matches on slug/name only for **dataset**, **table** and
**column**. For a table's CHILD records -- observation level, cloud table,
coverage, datetime range, update -- omitting `id` creates a BRAND NEW ROW every
time, so a second run silently multiplies them. Worse, once a table has two
coverages carrying a datetime range, `create_update_table` on it starts failing
outright. So this script reads the dataset first and reuses every id it finds,
and only creates what is genuinely absent.

Types and descriptions come from the models themselves and from the two glossary
modules, so this file holds no copy of either -- there is one definition of a
column's type (the `safe_cast` in its model) and one of its description.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import pathlib
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(
    0,
    os.path.expanduser("~/Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp"),
)

# pyrefly: ignore [missing-import]  # sibling module via sys.path
import gen_mg_schema as gen

# pyrefly: ignore [missing-import]  # sibling module via sys.path
import mg_column_glossary as glossary

# pyrefly: ignore [missing-import]  # sibling module via sys.path
import mg_table_glossary as tables

# pyrefly: ignore [missing-import]  # the databasis MCP server, via sys.path
import server

ENV = "staging"
DATASET_ID = "d3874769-bcbd-4ece-a38a-157ba1021514"  # slug `mides`
AREA_BR_MG = "6edeb8be-bf72-42c9-bdd7-5810808d2585"
STATUS_PUBLISHED = "e16221de-ac30-4926-83d3-de219998dab3"
ENTITY_DAY = "81f0c890-65a6-48a1-9523-af38d3f4af63"
ACCOUNT = "57"
GCP_PROJECT = "basedosdados"  # cloud tables name the PROD location, as the
GCP_DATASET = "world_wb_mides"  # existing MiDES tables already do
START_YEAR, END_YEAR = 2014, 2026

ENTITY = {
    "commitment": "5d0d3b72-49dd-42ef-be57-96658e844008",
    "verification": "574a640c-f3f1-4d3c-b67b-c290ee6dcf80",
    "payment": "7cd9f097-f7ad-4b8a-8c07-746b6fbef450",
    "procurement": "4cce9a0f-b438-442c-bb94-444445cb1a2d",
    "contract": "38e7435c-f2d1-4ddd-b010-283d0eb77f6c",
    "item": "5713c2f7-70d3-48f9-9b4c-5c531dc467ba",
    "company": "b585c285-3ad7-4b86-9c36-6195e4760a46",
    "person": "b4e76213-888b-40ea-b877-d82ce76d71a2",
    "amendment": "632035db-f0a6-4ca6-9194-586962613768",
    "law": "4cdf309e-395f-4bd9-9038-08969d2dc5d5",
    "other": "1b3a7364-3e76-4416-8af7-d52824da2d24",
}

# Grain of each table, in the vocabulary the backend actually has. `other` is
# used where the grain has no entity (an invoice, a budget appropriation) rather
# than forcing a near-miss; the existing MiDES tables declare only the domain
# entity, not municipality/year, and this follows that.
OBSERVATION_LEVELS: dict[str, list[str]] = {
    "alteracao_orcamentaria": ["amendment"],
    "contrato": ["contract"],
    "contrato_apostilamento": ["contract", "amendment"],
    "contrato_contabilizacao": ["contract", "commitment"],
    "contrato_credito": ["contract"],
    "contrato_item": ["contract", "item"],
    "contrato_rescisao": ["contract"],
    "contrato_termo_aditivo": ["contract", "amendment"],
    "contrato_termo_aditivo_item": ["contract", "amendment", "item"],
    "decreto": ["law"],
    "despesa_dotacao": ["other"],
    "dispensa": ["procurement"],
    "dispensa_cotacao": ["procurement", "item"],
    "dispensa_credenciado": ["procurement", "company"],
    "dispensa_dotacao": ["procurement"],
    "dispensa_fornecedor": ["procurement", "company"],
    "dispensa_item": ["procurement", "item"],
    "dispensa_responsavel": ["procurement", "person"],
    "empenho_credor": ["commitment", "company"],
    "empenho_fonte": ["commitment"],
    "lei_decreto": ["law"],
    "licitacao_comissao": ["procurement", "person"],
    "licitacao_cotacao": ["procurement", "item"],
    "licitacao_dotacao": ["procurement"],
    "licitacao_homologacao": ["procurement", "item"],
    "licitacao_julgamento": ["procurement", "item", "company"],
    "licitacao_parecer": ["procurement", "person"],
    "licitacao_quadro_societario": ["procurement", "person"],
    "licitacao_responsavel": ["procurement", "person"],
    "liquidacao_fonte": ["verification"],
    "liquidacao_nota_fiscal": ["verification"],
    "nota_fiscal": ["other"],
    "nota_fiscal_item": ["item"],
    "pagamento_movimento": ["payment"],
    "registro_preco_adesao": ["procurement"],
    "registro_preco_adesao_cotacao": ["procurement", "item"],
    "registro_preco_adesao_item": ["procurement", "item"],
    "registro_preco_adesao_vencedor": ["procurement", "company"],
    "restos_pagar": ["commitment"],
    "restos_pagar_credor": ["commitment", "company"],
    "restos_pagar_movimentacao": ["commitment"],
    "restos_pagar_movimentacao_credor": ["commitment", "company"],
    "restos_pagar_movimentacao_fonte": ["commitment"],
}

DIRECTORY = {
    "ano": "br_bd_diretorios_data_tempo.ano:ano",
    "mes": "br_bd_diretorios_data_tempo.mes:mes",
    "id_municipio": "br_bd_diretorios_brasil.municipio:id_municipio",
    "sigla_uf": "br_bd_diretorios_brasil.uf:sigla",
}

MEASUREMENT_UNIT = {"ano": "year", "mes": "month"}

CAST_RE = re.compile(
    r"as\s+(int64|string|float64|date|bool|numeric)\s*\)\s*as\s+([a-z_0-9]+)\s*$"
)


# Which column identifies each observation level, in preference order. Without
# this link the site renders the level's columns as "Nao informado"; and
# `bulk_upsert_columns` does not set it, so it needs its own `update_column`
# pass. Patterns are tried in order and the first column present wins.
OL_COLUMN: dict[str, list[str]] = {
    "commitment": [
        "id_empenho_bd",
        "id_restos_pagar_bd",
        "id_empenho",
        "id_rsp",
    ],
    "verification": ["id_liquidacao_bd", "id_liquidacao"],
    "payment": ["id_pagamento_bd", "id_pagamento"],
    "procurement": [
        "id_licitacao_bd",
        "id_dispensa_bd",
        "id_registro_preco_adesao_bd",
    ],
    "contract": ["id_contrato_bd", "id_contrato"],
    "item": [
        "id_contrato_item_bd",
        "id_contrato_termo_aditivo_item_bd",
        "id_dispensa_item_bd",
        "id_licitacao_item_bd",
        "id_nota_fiscal_item_bd",
        "id_registro_preco_adesao_item_bd",
    ],
    "company": [
        "numero_doc_credor",
        "numero_doc_fornecedor",
        "numero_doc_vencedor",
        "numero_doc_licitante",
        "numero_doc_credenciado",
        "numero_doc_emitente",
    ],
    "person": [
        "numero_doc_responsavel",
        "numero_doc_resp",
        "numero_doc_resp_parecer",
        "numero_documento",
        "numero_doc_signatario",
        "numero_doc_representante",
    ],
    "amendment": [
        "id_contrato_termo_aditivo_bd",
        "id_contrato_apostilamento_bd",
        "id_alteracao_orcamentaria_bd",
    ],
    "law": ["id_lei_decreto_bd", "id_decreto_bd"],
}


def ol_column(entity: str, table: str, names: list[str]) -> str | None:
    """The column that identifies `entity` in this table, or the table's own key."""
    for candidate in OL_COLUMN.get(entity, []):
        if candidate in names:
            return candidate
    own = f"id_{table}_bd"
    return own if own in names else None


PRIOR_QUERY = """query($ds: ID!, $slug: String!) {
  allTable(dataset_Id: $ds, slug: $slug, first: 1) {
    edges { node {
      id slug
      observationLevels { edges { node { id entity { slug } } } }
      cloudTables { edges { node { id } } }
      coverages { edges { node { id area { slug }
        datetimeRanges { edges { node { id startYear endYear interval } } } } } }
      updates { edges { node { id } } }
    } }
  }
}"""


def prior_state(slug: str) -> dict:
    """This table's existing ids, shaped like `get_dataset`'s table entry.

    NOT `get_dataset`: that returns every column of every table in the dataset,
    which on `mides` takes 80+ seconds and exceeds the client's own 60s read
    timeout once the 43 new tables are in. A per-table query is milliseconds.
    """
    edges = server._gql(
        PRIOR_QUERY, {"ds": DATASET_ID, "slug": slug}, env=ENV
    )["allTable"]["edges"]
    if not edges:
        return {}
    node = edges[0]["node"]
    return {
        "id": server._strip_id(node["id"]),
        "observation_levels": [
            {
                "id": server._strip_id(e["node"]["id"]),
                "entity_slug": e["node"]["entity"]["slug"],
            }
            for e in node["observationLevels"]["edges"]
        ],
        "cloud_tables": [
            {"id": server._strip_id(e["node"]["id"])}
            for e in node["cloudTables"]["edges"]
        ],
        "coverages": [
            {
                "id": server._strip_id(e["node"]["id"]),
                "area_slug": e["node"]["area"]["slug"],
                "datetime_ranges": [
                    {
                        "id": server._strip_id(r["node"]["id"]),
                        "start_year": r["node"]["startYear"],
                        "end_year": r["node"]["endYear"],
                        "interval": r["node"]["interval"],
                    }
                    for r in e["node"]["datetimeRanges"]["edges"]
                ],
            }
            for e in node["coverages"]["edges"]
        ],
        "updates": [
            {"id": server._strip_id(e["node"]["id"])}
            for e in node["updates"]["edges"]
        ],
    }


def typed_columns(path: str) -> list[tuple[str, str]]:
    """(name, BigQuery type) for a model's published columns, in order.

    Shares `gen_mg_schema.columns_of`'s depth-aware split so the two can never
    disagree about which columns a model publishes -- `sqlfmt` reflows the long
    casts across many lines, which defeats any line-anchored regex.
    """
    names = gen.columns_of(path)
    text = pathlib.Path(path).read_text(encoding="utf-8")
    types: dict[str, str] = {}
    flat = " ".join(line.split("--")[0] for line in text.split("\n"))
    for match in re.finditer(
        r"as\s+(int64|string|float64|date|bool|numeric)\s*\)\s*as\s+([a-z_0-9]+)",
        flat,
    ):
        types[match.group(2)] = match.group(1).upper()
    # `'MG' as sigla_uf` carries no cast
    return [(n, types.get(n, "STRING")) for n in names]


def columns_payload(path: str) -> list[dict]:
    out = []
    for name, bq_type in typed_columns(path):
        entry = {
            "name": name,
            "bigquery_type": bq_type,
            "description_pt": glossary.build_description(name, "pt"),
            "description_en": glossary.build_description(name, "en"),
            "description_es": glossary.build_description(name, "es"),
            "covered_by_dictionary": False,
            "has_sensitive_data": False,
        }
        if name in DIRECTORY:
            entry["directory_column"] = DIRECTORY[name]
        if name in MEASUREMENT_UNIT:
            entry["measurement_unit"] = MEASUREMENT_UNIT[name]
        out.append(entry)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--table", action="append", help="restrict to this table slug"
    )
    args = parser.parse_args()

    mg_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "mg"
    )
    slugs = sorted(
        fn[len("world_wb_mides__") : -len(".sql")]
        for fn in os.listdir(mg_dir)
        if fn.endswith(".sql")
    )
    if args.table:
        slugs = [s for s in slugs if s in args.table]

    for slug in slugs:
        path = os.path.join(mg_dir, f"world_wb_mides__{slug}.sql")
        cols = columns_payload(path)
        levels = OBSERVATION_LEVELS[slug]
        prior = prior_state(slug)
        if args.dry_run:
            print(
                f"  {slug:<34} {len(cols):>3} cols  OL={','.join(levels):<28} "
                f"{'EXISTS' if prior else 'new'}"
            )
            continue

        table = server.create_update_table(
            slug=slug,
            name_pt=tables.name(slug, "pt"),
            name_en=tables.name(slug, "en"),
            name_es=tables.name(slug, "es"),
            description_pt=tables.description(slug, "pt"),
            description_en=tables.description(slug, "en"),
            description_es=tables.description(slug, "es"),
            dataset_id=DATASET_ID,
            status_id=STATUS_PUBLISHED,
            published_by_ids=[ACCOUNT],
            data_cleaned_by_ids=[ACCOUNT],
            # NOT slug-idempotent: without the id a second run fails outright
            # with "Table com este Dataset e Slug ja existe".
            id=prior.get("id"),
            env=ENV,
        )
        table_id = (
            table["id"] if isinstance(table, dict) else json.loads(table)["id"]
        )

        server.bulk_upsert_columns(
            table_id=table_id,
            columns_json=json.dumps(cols, ensure_ascii=False),
            env=ENV,
        )

        # --- child records: reuse every id that already exists ---------------
        have_ol = {
            o.get("entity_slug"): o["id"]
            for o in prior.get("observation_levels", [])
        }
        for level in levels:
            server.create_update_observation_level(
                table_id=table_id,
                entity_id=ENTITY[level],
                id=have_ol.get(level),
                env=ENV,
            )

        # `bulk_upsert_columns` sets neither the partition flag nor the OL link.
        # `update_column`'s booleans default to False, so anything already true
        # on the column has to be re-passed in the same call or it is clobbered.
        names = [c["name"] for c in cols]
        # Only THIS table's ids are needed. `get_dataset` returns the whole
        # dataset -- every column of every table -- so calling it once per table
        # makes the run quadratic. Ask for just this table instead.
        fresh = server._gql(
            """query($id: ID!) {
                 allColumn(table_Id: $id, first: 500) {
                   edges { node { id name } } }
                 allObservationlevel(table_Id: $id, first: 20) {
                   edges { node { id entity { slug } } } }
               }""",
            {"id": table_id},
            env=ENV,
        )
        ol_by_entity = {
            e["node"]["entity"]["slug"]: server._strip_id(e["node"]["id"])
            for e in fresh["allObservationlevel"]["edges"]
        }
        # the real column ids -- `update_column` with an empty column_id tries to
        # CREATE, and then fails on the required bigqueryType
        col_id = {
            e["node"]["name"]: server._strip_id(e["node"]["id"])
            for e in fresh["allColumn"]["edges"]
        }
        linked: dict[str, str] = {}
        for level in levels:
            column = ol_column(level, slug, names)
            if column and level in ol_by_entity:
                linked[column] = ol_by_entity[level]
        for column in {"ano", *linked}:
            if column not in names:
                continue
            if column not in col_id:
                continue
            server.update_column(
                column_id=col_id[column],
                column_name=column,
                table_id=table_id,
                is_partition=(column == "ano"),
                observation_level_id=linked.get(column),
                env=ENV,
            )

        cloud = prior.get("cloud_tables") or [{}]
        server.create_update_cloud_table(
            table_id=table_id,
            gcp_project_id=GCP_PROJECT,
            gcp_dataset_id=GCP_DATASET,
            gcp_table_id=slug,
            id=cloud[0].get("id"),
            env=ENV,
        )

        covs = {c.get("area_slug"): c for c in prior.get("coverages", [])}
        mg_cov = covs.get("br_mg", {})
        coverage = server.create_update_coverage(
            table_id=table_id,
            area_id=AREA_BR_MG,
            id=mg_cov.get("id") or None,
            env=ENV,
        )
        coverage_id = (
            coverage["id"]
            if isinstance(coverage, dict)
            else json.loads(coverage)["id"]
        )
        ranges = mg_cov.get("datetime_ranges") or [{}]
        server.create_update_datetime_range(
            coverage_id=coverage_id,
            start_year=START_YEAR,
            end_year=END_YEAR,
            interval=1,
            id=ranges[0].get("id"),
            env=ENV,
        )

        updates = prior.get("updates") or [{}]
        server.create_update_update(
            entity_id=ENTITY_DAY,
            frequency=1,
            # the backend's `latest` is a DateTime, not a Date -- a bare
            # "YYYY-MM-DD" is rejected outright
            latest=datetime.datetime.now().replace(microsecond=0).isoformat(),
            table_id=table_id,
            id=updates[0].get("id"),
            env=ENV,
        )
        print(
            f"  {slug:<34} {len(cols):>3} cols  OL={','.join(levels):<28} registered"
        )

    print(
        f"\n{len(slugs)} tables processed ({'dry run' if args.dry_run else ENV})"
    )


if __name__ == "__main__":
    main()
