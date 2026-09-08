"""Register world_iati_activities metadata in the Data Basis backend.

    python register_metadata.py --env staging
    python register_metadata.py --env staging --publish   # flip to published
    python register_metadata.py --env prod                # after the PR merges

19 tables x (table, columns, observation levels, OL column links, cloud table,
coverage, datetime range, update) is far too many calls to make by hand, so this
imports the databasis MCP server module and calls its tool functions directly.
The server resolves its own backend credentials, exactly as it does when driven
over MCP; nothing here reads a credential file.

Re-running is safe. `create_update_*` is NOT idempotent on its own — omitting an
id creates a second observation level, cloud table, coverage or update rather
than updating the first — so every call here passes back the id it finds in
`get_dataset`.

Point ``DATABASIS_MCP_DIR`` at the checkout if it is not in the default place.
"""

import argparse
import csv
import json
import os
import sys
from datetime import UTC
from pathlib import Path

from common import ARCH_DIR
from tables import OL_COLUMN, TABLES

DATASET_SLUG = "iati_activities"
GCP_DATASET_ID = "world_iati_activities"
ORG_SLUG = "iati"
THEMES = ["economics", "government"]
TAGS = [
    "ajuda_internacional",
    "desenvolvimento",
    "doacao",
    "financiamento",
    "ong",
    "transparencia",
]

GCP_PROJECT = {
    "prod": "basedosdados",
    "staging": "basedosdados-dev",
    "dev": "basedosdados-dev",
}

DATASET_NAME = {
    "pt": "Atividades de cooperação e ajuda internacional (IATI)",
    "en": "Development cooperation and humanitarian aid activities (IATI)",
    "es": "Actividades de cooperación y ayuda internacional (IATI)",
}

DATASET_DESCRIPTION = {
    "pt": (
        "Atividades, transações, orçamentos e resultados de cooperação e ajuda "
        "internacional publicados por mais de duas mil organizações no padrão "
        "IATI. Os dados vêm do IATI Tables, o achatamento diário que a "
        "secretaria da IATI faz do acervo XML do Bulk Data Service. Cada linha "
        "carrega a licença declarada pela organização que a publicou, porque "
        "os dados IATI são licenciados por publicador e não por acervo; os 16 "
        "conjuntos não comerciais foram removidos. É um retrato do acervo "
        "atual, não uma série histórica: os publicadores reescrevem o próprio "
        "passado e o IATI Tables substitui todo o acervo a cada execução."
    ),
    "en": (
        "Activities, transactions, budgets and results of development "
        "cooperation and humanitarian aid published by more than two thousand "
        "organisations to the IATI standard. The data comes from IATI Tables, "
        "the IATI Secretariat's daily flattening of the Bulk Data Service XML "
        "corpus. Every row carries the licence its publishing organisation "
        "declared, because IATI data is licensed per publisher rather than per "
        "corpus; the 16 non-commercial datasets were removed. This is a "
        "snapshot of the current corpus, not a historical series: publishers "
        "restate their own past and IATI Tables replaces the whole corpus on "
        "each run."
    ),
    "es": (
        "Actividades, transacciones, presupuestos y resultados de cooperación y "
        "ayuda internacional publicados por más de dos mil organizaciones en el "
        "estándar IATI. Los datos provienen de IATI Tables, el aplanamiento "
        "diario que la secretaría de IATI hace del acervo XML del Bulk Data "
        "Service. Cada fila lleva la licencia declarada por la organización que "
        "la publicó, porque los datos IATI se licencian por publicador y no por "
        "acervo; los 16 conjuntos no comerciales fueron eliminados. Es una "
        "instantánea del acervo actual, no una serie histórica: los "
        "publicadores reescriben su propio pasado y IATI Tables sustituye todo "
        "el acervo en cada ejecución."
    ),
}

# Two sources, and each table is linked to exactly one of them. A table with two
# raw sources cannot run a recurring pipeline at all: client._raw_source_id
# raises "mais de um nó encontrado" and the poll fails before doing anything.
RAW_SOURCES = {
    "tables": {
        "name_pt": "IATI Tables",
        "name_en": "IATI Tables",
        "name_es": "IATI Tables",
        "url": "https://tables.iatistandard.org/",
        "description_pt": (
            "Achatamento diário do acervo XML da IATI em tabelas relacionais, "
            "publicado pela secretaria da IATI em CSV, SQLite e dumps "
            "PostgreSQL. A licença varia por publicador: 33,1% cc-by, 16,9% "
            "other-at, 15,5% cc-zero, 10,2% other-open, 9,7% odc-by e uma "
            "cauda, sobre 13.901 conjuntos registrados."
        ),
        "description_en": (
            "Daily flattening of the IATI XML corpus into relational tables, "
            "published by the IATI Secretariat as CSV, SQLite and PostgreSQL "
            "dumps. The licence varies by publisher: 33.1% cc-by, 16.9% "
            "other-at, 15.5% cc-zero, 10.2% other-open, 9.7% odc-by and a "
            "tail, across 13,901 registered datasets."
        ),
        "description_es": (
            "Aplanamiento diario del acervo XML de IATI en tablas "
            "relacionales, publicado por la secretaría de IATI en CSV, SQLite y "
            "volcados PostgreSQL. La licencia varía por publicador: 33,1% "
            "cc-by, 16,9% other-at, 15,5% cc-zero, 10,2% other-open, 9,7% "
            "odc-by y una cola, sobre 13.901 conjuntos registrados."
        ),
    },
    "bulk": {
        "name_pt": "IATI Bulk Data Service",
        "name_en": "IATI Bulk Data Service",
        "name_es": "IATI Bulk Data Service",
        "url": "https://bulk-data.iatistandard.org/",
        "description_pt": (
            "Cópia contínua de todos os conjuntos registrados na IATI, com "
            "índices em JSON dos conjuntos e das organizações publicadoras. É "
            "a única fonte da licença declarada por conjunto."
        ),
        "description_en": (
            "Continuously maintained copy of every dataset registered with "
            "IATI, with JSON indices of the datasets and the publishing "
            "organisations. It is the only source of the per-dataset declared "
            "licence."
        ),
        "description_es": (
            "Copia continua de todos los conjuntos registrados en IATI, con "
            "índices en JSON de los conjuntos y de las organizaciones "
            "publicadoras. Es la única fuente de la licencia declarada por "
            "conjunto."
        ),
    },
}

# registry_dataset is built from the Bulk Data Service index; every other table
# comes out of the IATI Tables export.
TABLE_SOURCE = {t: "tables" for t in TABLES}
TABLE_SOURCE["registry_dataset"] = "bulk"


def load_server():
    mcp_dir = Path(
        os.environ.get(
            "DATABASIS_MCP_DIR",
            Path.home() / "Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp",
        )
    )
    if not (mcp_dir / "server.py").exists():
        raise SystemExit(
            f"databasis MCP server not found at {mcp_dir}; set DATABASIS_MCP_DIR"
        )
    sys.path.insert(0, str(mcp_dir))
    import server

    return server


def columns_json(table: str) -> str:
    """The architecture CSV, in the shape bulk_upsert_columns wants.

    directory_column is deliberately absent: this dataset's foreign keys point
    at its own tables, not at a br_bd_diretorios_* directory, and an unresolved
    directory_column gets the whole column dropped at registration.
    """
    out = []
    with (ARCH_DIR / f"sheet_{table}.csv").open(encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            col = {
                "name": r["name"],
                "bigquery_type": r["bigquery_type"],
                "description_pt": r["description_pt"],
                "description_en": r["description_en"],
                "description_es": r["description_es"],
                "covered_by_dictionary": r["covered_by_dictionary"] == "yes",
                "has_sensitive_data": r["has_sensitive_data"] == "yes",
            }
            if r["measurement_unit"]:
                col["measurement_unit"] = r["measurement_unit"]
            for lang in ("pt", "en", "es"):
                if r[f"observations_{lang}"]:
                    col[f"observations_{lang}"] = r[f"observations_{lang}"]
            out.append(col)
    return json.dumps(out, ensure_ascii=False)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default="staging")
    ap.add_argument(
        "--publish",
        action="store_true",
        help="set the dataset to published (staging any time; prod only after "
        "the PR merges, table-approve runs and the prod tables are verified)",
    )
    ap.add_argument("--tables", nargs="*", default=list(TABLES))
    args = ap.parse_args()
    env = args.env
    srv = load_server()

    ids = srv.discover_ids(env=env, keys=["status", "availability", "license"])
    status = ids["status"]
    org = srv.lookup_id("organization", ORG_SLUG, env=env)["id"]
    area = srv.lookup_id("area", "world", env=env)["id"]
    english = srv.lookup_id("language", "en", env=env)["id"]
    account = srv.get_authenticated_account(env=env)["id"]
    themes = [srv.lookup_id("theme", t, env=env)["id"] for t in THEMES]
    tags = [srv.lookup_id("tag", t, env=env)["id"] for t in TAGS]
    entities = {
        slug: srv.lookup_id("entity", slug, env=env)["id"]
        for slug in sorted({e for t in TABLES.values() for e in t["entities"]})
    }

    existing = srv.get_dataset(DATASET_SLUG, env=env)
    dataset_id = srv.create_update_dataset(
        slug=DATASET_SLUG,
        name_pt=DATASET_NAME["pt"],
        name_en=DATASET_NAME["en"],
        name_es=DATASET_NAME["es"],
        description_pt=DATASET_DESCRIPTION["pt"],
        description_en=DATASET_DESCRIPTION["en"],
        description_es=DATASET_DESCRIPTION["es"],
        organization_ids=[org],
        theme_ids=themes,
        tag_ids=tags,
        status_id=status["published"]
        if args.publish
        else status["under_review"],
        id=existing.get("id"),
        env=env,
    )["id"]
    print(f"dataset {DATASET_SLUG} -> {dataset_id}")

    known_sources = {
        s["url"]: s["id"]
        for s in srv.get_raw_data_sources(DATASET_SLUG, env=env)
    }
    sources = {}
    for key, spec in RAW_SOURCES.items():
        sources[key] = srv.create_update_raw_data_source(
            dataset_id=dataset_id,
            name_pt=spec["name_pt"],
            name_en=spec["name_en"],
            name_es=spec["name_es"],
            description_pt=spec["description_pt"],
            description_en=spec["description_en"],
            description_es=spec["description_es"],
            url=spec["url"],
            # No single licence governs the corpus — IATI data is licensed per
            # publisher. The distribution is in the description.
            license_id=ids["license"]["unknown"],
            availability_id=ids["availability"]["online"],
            has_structured_data=True,
            is_free=True,
            contains_api=False,
            requires_registration=False,
            language_ids=[english],
            status_id=status["published"],
            id=known_sources.get(spec["url"]),
            env=env,
        )["id"]
        print(f"raw source {key} -> {sources[key]}")

    week = srv.lookup_id("entity", "week", env=env)["id"]
    prior = existing.get("tables", {}) if existing.get("found") else {}
    table_ids: dict[str, str] = {}
    ol_ids: dict[str, dict[str, str]] = {}
    for slug in args.tables:
        spec = TABLES[slug]
        was = prior.get(slug, {})
        table_id = srv.create_update_table(
            slug=slug,
            name_pt=spec["name_pt"],
            name_en=spec["name_en"],
            name_es=spec["name_es"],
            description_pt=spec["description_pt"],
            description_en=spec["description_en"],
            description_es=spec["description_es"],
            dataset_id=dataset_id,
            status_id=status["published"],
            published_by_ids=[account],
            data_cleaned_by_ids=[account],
            raw_data_source_ids=[sources[TABLE_SOURCE[slug]]],
            id=was.get("id"),
            env=env,
        )["id"]

        result = srv.bulk_upsert_columns(
            table_id=table_id, columns_json=columns_json(slug), env=env
        )

        by_entity = {
            ol["entity_id"]: ol["id"]
            for ol in was.get("observation_levels", [])
        }
        ol_ids[slug] = {}
        for entity_slug in spec["entities"]:
            entity_id = entities[entity_slug]
            ol_ids[slug][entity_slug] = srv.create_update_observation_level(
                table_id=table_id,
                entity_id=entity_id,
                id=by_entity.get(entity_id),
                env=env,
            )["id"]

        cloud = was.get("cloud_tables") or [{}]
        srv.create_update_cloud_table(
            table_id=table_id,
            gcp_project_id=GCP_PROJECT[env],
            gcp_dataset_id=GCP_DATASET_ID,
            gcp_table_id=slug,
            id=cloud[0].get("id"),
            env=env,
        )

        cov = was.get("coverages") or [{}]
        coverage_id = srv.create_update_coverage(
            table_id=table_id, area_id=area, id=cov[0].get("id"), env=env
        )["id"]
        ranges = cov[0].get("datetime_ranges") or [{}]
        start, end = spec["years"]
        srv.create_update_datetime_range(
            coverage_id=coverage_id,
            start_year=start,
            end_year=end,
            interval=1,
            id=ranges[0].get("id"),
            env=env,
        )

        # Table-anchored Update: when WE last refreshed, a wall clock. The
        # source-anchored Update (what IATI Tables last published) is written by
        # the recurring pipeline's commit_source_update_task.
        updates = was.get("updates") or [{}]
        srv.create_update_update(
            entity_id=week,
            frequency=1,
            latest=_today(),
            table_id=table_id,
            id=updates[0].get("id"),
            env=env,
        )
        table_ids[slug] = table_id
        print(
            f"  {slug:26s} id={table_id} columns={result.get('created', '?')}"
            f"/{result.get('updated', '?')} ols={len(spec['entities'])}"
        )

    # Second pass: link each observation level to the column that identifies
    # it. bulk_upsert_columns cannot do this, and update_column needs the real
    # column id — which only exists once the columns have been created, hence
    # the re-read.
    fresh = srv.get_dataset(DATASET_SLUG, env=env)
    for slug in args.tables:
        cols = {
            c["name"]: c["id"]
            for c in fresh["tables"][slug].get("columns", [])
        }
        for entity_slug, ol_id in ol_ids[slug].items():
            column = OL_COLUMN[entity_slug]
            if column not in cols:
                raise SystemExit(
                    f"{slug}: observation level {entity_slug} names column "
                    f"{column!r}, which the table does not have"
                )
            srv.update_column(
                column_id=cols[column],
                column_name=column,
                table_id=table_ids[slug],
                observation_level_id=ol_id,
                # update_column's booleans default to False and would otherwise
                # clobber the partition flag on `year`.
                is_partition=(column == "year"),
                env=env,
            )
        print(f"  linked {len(ol_ids[slug])} observation level(s) on {slug}")

    srv.reorder_tables(
        dataset_slug=DATASET_SLUG, table_slugs=list(TABLES), env=env
    )
    print(
        f"\ndone — https://{'' if env == 'prod' else env + '.'}basedosdados.org/dataset/{dataset_id}"
    )


def _today() -> str:
    from datetime import datetime

    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S")


if __name__ == "__main__":
    main()
