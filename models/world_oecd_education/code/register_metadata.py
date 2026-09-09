"""Register world_oecd_education metadata in the Data Basis backend.

    python register_metadata.py --env staging     # default
    python register_metadata.py --env prod        # only after the checkpoint

Idempotent by construction. ``create_update_*`` matches on ``id``, not on slug, so
re-running without first looking up what exists creates duplicate observation
levels, cloud tables, coverages and updates. Everything here reads the current
state with ``get_dataset`` and passes the existing id back.

The dataset is created ``under_review``: that hides it from the production
frontend, so metadata registered before the PR merges -- with prod cloud tables
that do not exist yet -- cannot leak publicly. On staging it is published at the
end, because the staging frontend is not the public site and the reviewer should
see the dataset as it will appear.
"""

import argparse
import csv
import json
import sys

sys.path.insert(
    0, "/Users/rdahis/Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp"
)
import server
from common import ARCH_DIR, CODE_DIR, DATASET_ID
from tables import TABLES

SLUG = "education"
GCP_PROJECT = {
    "staging": "basedosdados-dev",
    "dev": "basedosdados-dev",
    "prod": "basedosdados",
}

# Content tags only. The dataset's theme is already "education", so a tag that
# merely restates it adds nothing, and area/organisation are separate metadata.
TAGS = [
    "matricula",
    "docente",
    "escolaridade",
    "salario",
    "financiamento",
    "gasto",
    "indicadores_educacionais",
    "educacao_superior",
    "trabalho",
]

NAME_PT = "OCDE: educação"
NAME_EN = "OECD: education"
NAME_ES = "OCDE: educación"

DESC_PT = (
    "Estatísticas de educação da OCDE reunidas na coleta UOE (OCDE, Instituto de "
    "Estatística da UNESCO e Eurostat), na publicação Education at a Glance e na "
    "Pesquisa Internacional sobre Ensino e Aprendizagem (TALIS). Cobre matrículas e "
    "concluintes, pessoal docente, financiamento da educação, salários e jornada de "
    "professores, tempo de instrução e os resultados de escolaridade no mercado de "
    "trabalho, para países da OCDE e parceiros. Cada tabela é um cubo longo: uma "
    "observação por combinação de dimensões, com o código de cada dimensão resolvido "
    "na tabela dicionario."
)
DESC_EN = (
    "OECD education statistics from the UOE data collection (OECD, UNESCO Institute "
    "for Statistics and Eurostat), the Education at a Glance publication and the "
    "Teaching and Learning International Survey (TALIS). Covers enrolment and "
    "graduation, teaching personnel, education finance, teacher salaries and working "
    "time, instruction time, and the labour market outcomes of educational "
    "attainment, for OECD and partner countries. Each table is a long cube: one "
    "observation per combination of dimensions, with each dimension's code resolved "
    "in the dicionario table."
)
DESC_ES = (
    "Estadísticas de educación de la OCDE reunidas en la recolección UOE (OCDE, "
    "Instituto de Estadística de la UNESCO y Eurostat), en la publicación Education "
    "at a Glance y en la Encuesta Internacional sobre Enseñanza y Aprendizaje "
    "(TALIS). Cubre matrículas y graduados, personal docente, financiamiento de la "
    "educación, salarios y jornada de los profesores, tiempo de instrucción y los "
    "resultados de la escolaridad en el mercado laboral, para países de la OCDE y "
    "socios. Cada tabla es un cubo largo: una observación por combinación de "
    "dimensiones, con el código de cada dimensión resuelto en la tabla dicionario."
)

RAW_SOURCE = dict(
    name_pt="API SDMX da OCDE",
    name_en="OECD SDMX API",
    name_es="API SDMX de la OCDE",
    url="https://sdmx.oecd.org/public/rest",
    description_pt=(
        "Interface SDMX REST pela qual a OCDE publica todas as suas bases "
        "estatísticas. Os 141 dataflows de educação são recortes de 15 cubos, e cada "
        "tabela deste conjunto vem do dataflow que carrega o cubo inteiro."
    ),
    description_en=(
        "The SDMX REST interface through which the OECD publishes all of its "
        "statistical databases. Its 141 education dataflows are views over 15 cubes, "
        "and each table here comes from the dataflow carrying the whole cube."
    ),
    description_es=(
        "Interfaz SDMX REST por la que la OCDE publica todas sus bases estadísticas. "
        "Los 141 dataflows de educación son recortes de 15 cubos, y cada tabla de "
        "este conjunto proviene del dataflow que contiene el cubo completo."
    ),
)

# Grain of each cube, as backend entity slugs. Every cube is a country-year panel
# except the subnational finance one, whose areas are NUTS-style regions.
OBSERVATION_LEVELS = {
    slug: (
        ["region", "year"]
        if slug == "finance_subnational"
        else ["country", "year"]
    )
    for slug in TABLES
}


def arch_columns(slug):
    with (ARCH_DIR / f"{slug}.csv").open() as f:
        return list(csv.DictReader(f))


def columns_payload(slug):
    """bulk_upsert_columns payload, in architecture order."""
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


def dicionario_columns():
    """The dicionario's five columns, which have no architecture CSV."""
    spec = [
        (
            "id_tabela",
            "Tabela deste conjunto a que a entrada se refere",
            "Table of this dataset the entry refers to",
            "Tabla de este conjunto a la que se refiere la entrada",
        ),
        (
            "nome_coluna",
            "Coluna codificada a que a entrada se refere",
            "Coded column the entry refers to",
            "Columna codificada a la que se refiere la entrada",
        ),
        (
            "chave",
            "Código tal como aparece na coluna",
            "Code exactly as it appears in the column",
            "Código tal como aparece en la columna",
        ),
        (
            "cobertura_temporal",
            "Cobertura temporal da entrada, vazia quando igual à da tabela",
            "Temporal coverage of the entry, empty when the same as the table's",
            "Cobertura temporal de la entrada, vacía cuando es igual a la de la tabla",
        ),
        (
            "valor",
            "Rótulo que a OCDE publica para o código",
            "Label the OECD publishes for the code",
            "Etiqueta que la OCDE publica para el código",
        ),
    ]
    return [
        {
            "name": n,
            "bigquery_type": "STRING",
            "description": pt,
            "description_en": en,
            "description_es": es,
            "covered_by_dictionary": False,
            "directory_column": "",
            "measurement_unit": "",
            "has_sensitive_data": False,
            "observations": "",
            "observations_en": "",
            "observations_es": "",
            "is_partition": False,
            "order": i,
        }
        for i, (n, pt, en, es) in enumerate(spec)
    ]


def world_area(env):
    """The id of the "world" area -- every cube here is cross-country."""
    q = 'query { allArea(slug: "world") { edges { node { id } } } }'
    node = server._gql(q, {}, env=env)["allArea"]["edges"][0]["node"]["id"]
    # The GraphQL layer returns a prefixed global id ("AreaNode:<uuid>"); the
    # REST-shaped create_update_* helpers want the bare uuid.
    return node.split(":", 1)[1] if ":" in node else node


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--env", default="staging", choices=["staging", "dev", "prod"]
    )
    ap.add_argument(
        "--publish",
        action="store_true",
        help=(
            "flip the dataset to published. Safe on staging, which is not the "
            "public site; on prod, only after the PR has merged, table-approve "
            "has materialised the tables, and they have been verified."
        ),
    )
    args = ap.parse_args()
    env = args.env

    # Explicit keys: a bare discover_ids() also fetches allEntityCategory, which
    # this backend spells allEntitycategory, and the whole call 400s.
    ids = server.discover_ids(
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
    if isinstance(ids, str):
        ids = json.loads(ids)
    account = server.get_authenticated_account(env=env)
    if isinstance(account, str):
        account = json.loads(account)
    account_id = account.get("id") or account["account"]["id"]

    measured = json.loads((CODE_DIR / "measured.json").read_text())

    existing = server.get_dataset(SLUG, env=env)
    if isinstance(existing, str):
        existing = json.loads(existing)
    dataset_id = (existing or {}).get("id")

    dataset = server.create_update_dataset(
        slug=SLUG,
        name_pt=NAME_PT,
        name_en=NAME_EN,
        name_es=NAME_ES,
        description_pt=DESC_PT,
        description_en=DESC_EN,
        description_es=DESC_ES,
        organization_ids=[ids["organization"]["oecd"]],
        theme_ids=[ids["theme"]["education"]],
        tag_ids=[ids["tag"][t] for t in TAGS if t in ids["tag"]],
        status_id=ids["status"][
            "published" if args.publish else "under_review"
        ],
        id=dataset_id,
        env=env,
    )
    if isinstance(dataset, str):
        dataset = json.loads(dataset)
    dataset_id = dataset.get("id", dataset_id)
    print(
        f"dataset {SLUG} -> {dataset_id} "
        f"({'published' if args.publish else 'under_review'})"
    )

    # create_update_raw_data_source matches on id, so without this lookup every
    # re-run adds another copy. A table linked to two raw sources also cannot run
    # a recurring pipeline at all -- client._raw_source_id raises on 2+.
    existing_raw = server.get_raw_data_sources(SLUG, env=env)
    if isinstance(existing_raw, str):
        existing_raw = json.loads(existing_raw)
    raw_id_prev = next(
        (
            r["id"]
            for r in (existing_raw or [])
            if isinstance(r, dict) and r.get("url") == RAW_SOURCE["url"]
        ),
        None,
    )

    raw = server.create_update_raw_data_source(
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
    if isinstance(raw, str):
        raw = json.loads(raw)
    raw_id = raw.get("id")
    print(f"raw data source -> {raw_id}")

    state = server.get_dataset(SLUG, env=env)
    if isinstance(state, str):
        state = json.loads(state)
    # get_dataset returns tables as a dict keyed by slug, not a list.
    by_slug = (state or {}).get("tables") or {}

    order = [*TABLES, "dicionario"]
    table_ols = {}
    for slug in order:
        spec = TABLES.get(slug)
        prev = by_slug.get(slug, {})
        if slug == "dicionario":
            names = ("Dicionário", "Dictionary", "Diccionario")
            descs = (
                "Rótulos das colunas codificadas deste conjunto, extraídos das listas "
                "de códigos SDMX publicadas pela OCDE",
                "Labels for this dataset's coded columns, taken from the SDMX code "
                "lists the OECD publishes",
                "Etiquetas de las columnas codificadas de este conjunto, extraídas de "
                "las listas de códigos SDMX publicadas por la OCDE",
            )
            payload = dicionario_columns()
        else:
            names = (spec["name_pt"], spec["name_en"], spec["name_es"])
            descs = (
                spec["description_pt"],
                spec["description_en"],
                spec["description_es"],
            )
            payload = columns_payload(slug)

        table = server.create_update_table(
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
        if isinstance(table, str):
            table = json.loads(table)
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
            ol = server.create_update_observation_level(
                table_id=table_id,
                entity_id=ids["entity"][entity],
                id=prev_ols.get(entity, {}).get("id"),
                env=env,
            )
            if isinstance(ol, str):
                ol = json.loads(ol)
            ol_ids[entity] = ol.get("id")

        server.bulk_upsert_columns(
            table_id=table_id,
            columns_json=json.dumps(payload, ensure_ascii=False),
            env=env,
        )

        table_ols[slug] = (table_id, ol_ids)

        if slug != "dicionario":
            years = sorted(
                int(y) for y in measured[slug].get("years", []) or []
            )
            cov = server.create_update_coverage(
                table_id=table_id,
                area_id=world_area(env),
                id=(prev.get("coverages") or [{}])[0].get("id"),
                env=env,
            )
            if isinstance(cov, str):
                cov = json.loads(cov)
            if years:
                # Pass the existing range's id: create_update_* matches on id, so
                # without it every re-run appends another identical range rather
                # than updating the one that is there.
                prev_range = (
                    (
                        (prev.get("coverages") or [{}])[0].get(
                            "datetime_ranges"
                        )
                        or [{}]
                    )[0]
                ).get("id")
                server.create_update_datetime_range(
                    coverage_id=cov["id"],
                    start_year=years[0],
                    end_year=years[-1],
                    interval=1,
                    id=prev_range,
                    env=env,
                )
        print(f"  {slug:24s} table={table_id} cols={len(payload)}")

    # bulk_upsert_columns does not link observation levels; that is a separate
    # per-column update, and without it the site renders the level's columns as
    # "Não informado". Done in one pass over a single fetch rather than
    # re-reading the dataset per table.
    fresh = server.get_dataset(SLUG, env=env)
    if isinstance(fresh, str):
        fresh = json.loads(fresh)
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
        link = (
            {"year": "year", "reference_area": "region"}
            if slug == "finance_subnational"
            else {"year": "year", "country_iso3_code": "country"}
        )
        for column, entity in link.items():
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
