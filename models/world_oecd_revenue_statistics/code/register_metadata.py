"""Register world_oecd_revenue_statistics metadata in the Data Basis backend.

    python register_metadata.py --env staging            # default
    python register_metadata.py --env staging --publish  # step 9b: publish on staging
    python register_metadata.py --env prod               # only after the checkpoint + merge

This dataset REPURPOSES an existing empty shell rather than creating a new one:
`oecd_revenue_statistics_in_latin_america_and_the_caribbean` (id below) is the LAC
regional subset of this same database, which the global database subsumes. The
first run renames that shell's slug/names/description to the global dataset; the
`oecd_tax_database` shell is a different OECD product (tax rates) and is left alone.

Idempotent: create_update_* matches on id, so every id that already exists is read
back and passed in. The dataset is created `under_review` (hidden from the prod
frontend); on staging it is published at the end (--publish), because staging is
not the public site.
"""

import argparse
import csv
import json
import sys
from pathlib import Path

sys.path.insert(
    0, "/Users/rdahis/Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp"
)
import server

SLUG = "world_oecd_revenue_statistics"
DATASET_ID = "world_oecd_revenue_statistics"  # gcp_dataset_id
# The empty LAC shell to repurpose into the global dataset (same id across envs).
REPURPOSE_ID = "b0164aa7-1cbe-455c-8776-23de5022f4e8"
ARCH_DIR = Path(__file__).resolve().parent / "architecture"

GCP_PROJECT = {
    "staging": "basedosdados-dev",
    "dev": "basedosdados-dev",
    "prod": "basedosdados",
}
START_YEAR, END_YEAR = (
    1990,
    2024,
)  # verified from the data; the pipeline refreshes it

TABLES = ["revenue"]
OBSERVATION_LEVELS = {"revenue": ["country", "year"]}

NAME_PT = "OCDE: estatísticas globais de receita"
NAME_EN = "OECD: global revenue statistics"
NAME_ES = "OCDE: estadísticas globales de ingresos"

DESC_PT = (
    "Receitas tributárias e não tributárias por categoria da classificação da OCDE, "
    "nível de governo, país e ano, do Banco de Dados Global de Estatísticas de "
    "Receita da OCDE. Aplica a metodologia de Estatísticas de Receita da OCDE a 146 "
    "economias da OCDE e não pertencentes à OCDE entre 1990 e 2024, expressando cada "
    "categoria como percentagem do PIB, percentagem da receita do mesmo nível de "
    "governo, percentagem da categoria de imposto superior, em moeda nacional e em "
    "dólares dos Estados Unidos. A classificação de impostos é hierárquica: use a "
    "categoria superior para evitar dupla contagem ao somar categorias."
)
DESC_EN = (
    "Tax and non-tax revenue by category of the OECD classification, level of "
    "government, country and year, from the OECD Global Revenue Statistics Database. "
    "It applies the OECD Revenue Statistics methodology to 146 OECD and non-OECD "
    "economies for 1990 to 2024, expressing each category as a percentage of GDP, a "
    "percentage of revenue at the same government level, a percentage of the parent "
    "tax category, in national currency and in US dollars. The tax classification is "
    "hierarchical: use the parent category to avoid double-counting when summing "
    "categories."
)
DESC_ES = (
    "Ingresos tributarios y no tributarios por categoría de la clasificación de la "
    "OCDE, nivel de gobierno, país y año, de la Base de Datos Global de Estadísticas "
    "de Ingresos de la OCDE. Aplica la metodología de Estadísticas de Ingresos de la "
    "OCDE a 146 economías de la OCDE y no pertenecientes a la OCDE entre 1990 y 2024, "
    "expresando cada categoría como porcentaje del PIB, porcentaje del ingreso del "
    "mismo nivel de gobierno, porcentaje de la categoría de impuesto superior, en "
    "moneda nacional y en dólares estadounidenses. La clasificación de impuestos es "
    "jerárquica: use la categoría superior para evitar el doble conteo al sumar "
    "categorías."
)

TABLE_META = {
    "revenue": (
        ("Receita", "Revenue", "Ingreso"),
        (DESC_PT, DESC_EN, DESC_ES),
    ),
    "dicionario": (
        ("Dicionário", "Dictionary", "Diccionario"),
        (
            "Rótulos das colunas codificadas deste conjunto, extraídos das listas de "
            "códigos SDMX publicadas pela OCDE",
            "Labels for this dataset's coded columns, taken from the SDMX code lists "
            "the OECD publishes",
            "Etiquetas de las columnas codificadas de este conjunto, extraídas de las "
            "listas de códigos SDMX publicadas por la OCDE",
        ),
    ),
}

RAW_SOURCE = dict(
    name_pt="API SDMX da OCDE",
    name_en="OECD SDMX API",
    name_es="API SDMX de la OCDE",
    url="https://sdmx.oecd.org/public/rest",
    description_pt=(
        "Interface SDMX REST pela qual a OCDE publica todas as suas bases "
        "estatísticas. Esta tabela vem do dataflow DF_RSGLOBAL (DSD_REV_COMP_GLOBAL), "
        "o cubo comparativo do Banco de Dados Global de Estatísticas de Receita."
    ),
    description_en=(
        "The SDMX REST interface through which the OECD publishes all of its "
        "statistical databases. This table comes from the DF_RSGLOBAL dataflow "
        "(DSD_REV_COMP_GLOBAL), the comparative cube of the Global Revenue Statistics "
        "Database."
    ),
    description_es=(
        "Interfaz SDMX REST por la que la OCDE publica todas sus bases estadísticas. "
        "Esta tabla proviene del dataflow DF_RSGLOBAL (DSD_REV_COMP_GLOBAL), el cubo "
        "comparativo de la Base de Datos Global de Estadísticas de Ingresos."
    ),
)

# Content tags (subject matter only; org/theme/area are separate metadata). Each is
# a candidate list, most-preferred first: the two backends do not share a vocabulary
# (prod uses English kebab-case, staging carries legacy Portuguese). First existing
# slug wins; a concept with no match is dropped rather than inventing a duplicate.
TAG_CANDIDATES = [
    ["tax", "imposto"],
    ["taxes", "impostos"],
    ["revenue", "receita"],
    ["taxation", "taxacao"],
    ["public-finance", "financas-publicas"],
    ["revenue-collection", "arrecadacao"],
    ["government", "governo"],
]


def resolve_tags(vocabulary):
    out = []
    for candidates in TAG_CANDIDATES:
        for slug in candidates:
            if slug in vocabulary:
                out.append(vocabulary[slug])
                break
    return out


def arch_columns(slug):
    with (ARCH_DIR / f"{slug}.csv").open(encoding="utf-8") as f:
        return list(csv.DictReader(f))


def columns_payload(slug):
    out = []
    for order, r in enumerate(arch_columns(slug)):
        out.append(
            {
                "name": r["name"],
                "bigquery_type": r["bigquery_type"],
                "description": r["description_pt"],
                "description_en": r["description_en"],
                "description_es": r["description_es"],
                "covered_by_dictionary": r["covered_by_dictionary"] == "yes",
                "directory_column": r["directory_column"],
                "measurement_unit": r["measurement_unit"],
                "has_sensitive_data": False,
                "observations": r["observations_pt"],
                "observations_en": r["observations_en"],
                "observations_es": r["observations_es"],
                "is_partition": r["name"] == "year",
                "order": order,
            }
        )
    return out


def world_area(env):
    q = 'query { allArea(slug: "world") { edges { node { id } } } }'
    node = server._gql(q, {}, env=env)["allArea"]["edges"][0]["node"]["id"]
    return node.split(":", 1)[1] if ":" in node else node


def _j(x):
    return json.loads(x) if isinstance(x, str) else x


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--env", default="staging", choices=["staging", "dev", "prod"]
    )
    ap.add_argument("--publish", action="store_true")
    args = ap.parse_args()
    env = args.env

    ids = _j(
        server.discover_ids(
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
    account = _j(server.get_authenticated_account(env=env))
    account_id = account.get("id") or account["account"]["id"]

    existing = _j(server.get_dataset(SLUG, env=env)) or {}
    dataset_id = existing.get("id") or REPURPOSE_ID  # repurpose the LAC shell

    dataset = _j(
        server.create_update_dataset(
            slug=SLUG,
            name_pt=NAME_PT,
            name_en=NAME_EN,
            name_es=NAME_ES,
            description_pt=DESC_PT,
            description_en=DESC_EN,
            description_es=DESC_ES,
            organization_ids=[ids["organization"]["oecd"]],
            theme_ids=[ids["theme"]["economics"], ids["theme"]["government"]],
            tag_ids=resolve_tags(ids["tag"]),
            status_id=ids["status"][
                "published" if args.publish else "under_review"
            ],
            id=dataset_id,
            env=env,
        )
    )
    dataset_id = dataset.get("id", dataset_id)
    print(
        f"dataset {SLUG} -> {dataset_id} ({'published' if args.publish else 'under_review'})"
    )

    existing_raw = _j(server.get_raw_data_sources(SLUG, env=env)) or []
    raw_id_prev = next(
        (
            r["id"]
            for r in existing_raw
            if isinstance(r, dict) and r.get("url") == RAW_SOURCE["url"]
        ),
        None,
    )
    raw = _j(
        server.create_update_raw_data_source(
            id=raw_id_prev,
            dataset_id=dataset_id,
            license_id=ids["license"]["cc_by_igo"],
            availability_id=ids["availability"]["online"],
            has_structured_data=True,
            contains_api=True,
            is_free=True,
            requires_registration=False,
            env=env,
            **RAW_SOURCE,
        )
    )
    raw_id = raw.get("id")
    print(f"raw data source -> {raw_id}")

    state = _j(server.get_dataset(SLUG, env=env)) or {}
    by_slug = state.get("tables") or {}
    order = [*TABLES, "dicionario"]
    table_ols = {}
    for slug in order:
        prev = by_slug.get(slug, {})
        names, descs = TABLE_META[slug]
        payload = columns_payload(slug)
        table = _j(
            server.create_update_table(
                slug=slug,
                name_pt=names[0],
                name_en=names[1],
                name_es=names[2],
                description_pt=descs[0],
                description_en=descs[1],
                description_es=descs[2],
                dataset_id=dataset_id,
                status_id=ids["status"]["published"],
                published_by_ids=[account_id],
                data_cleaned_by_ids=[account_id],
                raw_data_source_ids=[raw_id] if raw_id else None,
                id=prev.get("id"),
                env=env,
            )
        )
        table_id = table.get("id", prev.get("id"))

        server.create_update_cloud_table(
            table_id=table_id,
            gcp_project_id=GCP_PROJECT[env],
            gcp_dataset_id=DATASET_ID,
            gcp_table_id=slug,
            id=(prev.get("cloud_tables") or [{}])[0].get("id"),
            env=env,
        )

        ol_ids = {}
        prev_ols = {
            o.get("entity_slug"): o
            for o in (prev.get("observation_levels") or [])
        }
        for entity in OBSERVATION_LEVELS.get(slug, []):
            ol = _j(
                server.create_update_observation_level(
                    table_id=table_id,
                    entity_id=ids["entity"][entity],
                    id=prev_ols.get(entity, {}).get("id"),
                    env=env,
                )
            )
            ol_ids[entity] = ol.get("id")

        server.bulk_upsert_columns(
            table_id=table_id,
            columns_json=json.dumps(payload, ensure_ascii=False),
            env=env,
        )
        table_ols[slug] = (table_id, ol_ids)

        if slug != "dicionario":
            cov = _j(
                server.create_update_coverage(
                    table_id=table_id,
                    area_id=world_area(env),
                    id=(prev.get("coverages") or [{}])[0].get("id"),
                    env=env,
                )
            )
            prev_range = (
                (
                    (prev.get("coverages") or [{}])[0].get("datetime_ranges")
                    or [{}]
                )[0]
            ).get("id")
            server.create_update_datetime_range(
                coverage_id=cov["id"],
                start_year=START_YEAR,
                end_year=END_YEAR,
                interval=1,
                id=prev_range,
                env=env,
            )
        print(
            f"  {slug:12s} table={table_id} cols={len(payload)} ols={list(ol_ids)}"
        )

    # bulk_upsert_columns does not link OLs; that is a separate per-column update, or
    # the site renders the level's columns as "Não informado".
    fresh = _j(server.get_dataset(SLUG, env=env)) or {}
    linked = 0
    for slug, (table_id, ol_ids) in table_ols.items():
        if slug == "dicionario" or not ol_ids:
            continue
        cols = {
            c["name"]: c["id"]
            for c in (
                (fresh.get("tables") or {}).get(slug, {}).get("columns") or []
            )
        }
        for column, entity in {
            "year": "year",
            "country_iso3_code": "country",
        }.items():
            if column in cols and ol_ids.get(entity):
                server.update_column(
                    column_id=cols[column],
                    column_name=column,
                    table_id=table_id,
                    observation_level_id=ol_ids[entity],
                    is_partition=(column == "year"),
                    env=env,
                )
                linked += 1
    print(f"linked {linked} columns to observation levels")

    server.reorder_tables(dataset_slug=SLUG, table_slugs=order, env=env)
    print(f"\nregistered {len(order)} tables in {env}")


if __name__ == "__main__":
    main()
