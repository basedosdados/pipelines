#!/usr/bin/env python
"""Register ``us_osha_enforcement`` metadata in the Data Basis backend.

    ~/.venvs/bd-pipelines/bin/python \
        models/us_osha_enforcement/code/register_metadata.py --env staging

Idempotent by construction: every record is looked up before it is written, and
an existing id is passed back on update. ``create_update_*`` is *not*
idempotent on its own — omitting the id creates a duplicate observation level,
cloud table, coverage or update on every re-run.

The script talks to the backend through the databasis MCP server module rather
than the MCP tool surface, so the 138-column payloads never have to be pasted
through a conversation.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import logging
import sys
from datetime import date
from pathlib import Path

MCP = "/Users/rdahis/Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp"
HERE = Path(__file__).resolve().parent
DATASET_ID = "us_osha_enforcement"
SLUG = "enforcement"
ORG_SLUG = "osha"

#: Newest inspection open_date in the source files, for the raw source Update.
SOURCE_MAX_DATE = "2026-09-03T00:00:00+00:00"

#: Wall-clock date of this onboarding, for each table Update.
TODAY = "2026-09-09T00:00:00+00:00"

log = logging.getLogger("register_metadata")

# Observation levels per table, as (entity_slug, [identifying columns]).
#
# There is no `inspection` entity in the shared vocabulary; `audit` is the
# closest published one — a regulatory examination of an establishment — and is
# used rather than minting a near-duplicate. Every level is linked to the
# columns that identify it, or the site renders the level's columns as "Não
# informado".
OBSERVATION_LEVELS: dict[str, list[tuple[str, list[str]]]] = {
    "inspection": [
        ("audit", ["inspection_id"]),
        ("state", ["site_state"]),
        ("year", ["year"]),
    ],
    "violation": [
        ("citation", ["inspection_id", "citation_id"]),
        ("year", ["year"]),
    ],
    "violation_event": [
        ("citation", ["inspection_id", "citation_id"]),
        ("year", ["year"]),
    ],
    "violation_text": [
        ("citation", ["inspection_id", "citation_id"]),
        ("year", ["year"]),
    ],
    "related_activity": [("audit", ["inspection_id"]), ("year", ["year"])],
    "emphasis_code": [("audit", ["inspection_id"]), ("year", ["year"])],
    "optional_code_info": [("audit", ["inspection_id"]), ("year", ["year"])],
    "accident": [("crash", ["accident_id"]), ("year", ["year"])],
    "accident_injury": [
        ("person", ["accident_id", "inspection_id", "injury_line_number"]),
        ("year", ["year"]),
    ],
    "accident_narrative": [("crash", ["accident_id"]), ("year", ["year"])],
    "dicionario": [],
}

# Temporal coverage per table, measured from the cleaned data.
# Historical start of each table's coverage, measured from the cleaned
# partitions. Only the *start* is set here: the pipeline's
# `compute_coverage_ranges` writes the free range's end and the whole pro range
# on every run, and never touches the free start, so whatever is registered at
# onboarding persists.
COVERAGE_START: dict[str, tuple[int, int | None, int | None] | tuple[()]] = {
    "inspection": (1970, 6, 20),
    "violation": (1972, 3, 15),
    "violation_event": (1972, None, None),
    "violation_text": (1984, None, None),
    "related_activity": (1970, None, None),
    "emphasis_code": (1972, None, None),
    "optional_code_info": (1973, None, None),
    "accident": (1972, 9, 21),
    "accident_injury": (1972, None, None),
    "accident_narrative": (1972, None, None),
    "dicionario": (),
}

# Newest value of each table's coverage date column, measured in BigQuery after
# the dev build. The pipeline recomputes this from the data on every run; these
# are only the values registered at onboarding, so the dataset does not sit with
# an empty pro window until the first refresh.
SOURCE_END: dict[str, date] = {
    "inspection": date(2026, 9, 3),
    "violation": date(2026, 8, 6),
    "violation_event": date(2026, 1, 1),
    "violation_text": date(2026, 1, 1),
    "related_activity": date(2026, 1, 1),
    "emphasis_code": date(2026, 1, 1),
    "optional_code_info": date(2026, 1, 1),
    "accident": date(2025, 3, 28),
    "accident_injury": date(2025, 1, 1),
    "accident_narrative": date(2025, 1, 1),
}

DATASET_TEXT = {
    "name_pt": "Fiscalização da OSHA",
    "name_en": "OSHA Enforcement",
    "name_es": "Fiscalización de OSHA",
    "description_pt": (
        "Registro completo da fiscalização de segurança e saúde no trabalho "
        "nos Estados Unidos, desde 1972: cada inspeção realizada pela OSHA ou "
        "por um plano estadual, as violações citadas, as multas propostas e "
        "revisadas, e os acidentes e lesões investigados. Onze tabelas ligadas "
        "pelo número da inspeção (activity number). A multa corrente é um "
        "valor móvel: é contestada e revisada por anos após a citação e só se "
        "estabiliza quando o caso é encerrado — o histórico completo está em "
        "violation_event. Fonte: Catálogo de Dados de Fiscalização do "
        "Departamento do Trabalho dos Estados Unidos."
    ),
    "description_en": (
        "The complete record of United States workplace safety and health "
        "enforcement since 1972: every inspection carried out by OSHA or a "
        "state plan, the violations cited, the penalties proposed and revised, "
        "and the incidents and injuries investigated. Eleven tables linked by "
        "the inspection's activity number. The current penalty is a moving "
        "figure: it is contested and revised for years after the citation and "
        "only settles when the case closes — the full history is in "
        "violation_event. Source: the United States Department of Labor "
        "Enforcement Data Catalog."
    ),
    "description_es": (
        "Registro completo de la fiscalización de seguridad y salud laboral en "
        "los Estados Unidos desde 1972: cada inspección realizada por OSHA o "
        "por un plan estatal, las violaciones citadas, las multas propuestas y "
        "revisadas, y los accidentes y lesiones investigados. Once tablas "
        "vinculadas por el número de inspección (activity number). La multa "
        "corriente es un valor móvil: se impugna y revisa durante años tras la "
        "citación y solo se estabiliza al cerrarse el caso — el historial "
        "completo está en violation_event. Fuente: Catálogo de Datos de "
        "Fiscalización del Departamento de Trabajo de los Estados Unidos."
    ),
}

RAW_SOURCES = [
    {
        "name_pt": "Catálogo de Dados de Fiscalização do DOL — OSHA",
        "name_en": "DOL Enforcement Data Catalog — OSHA",
        "name_es": "Catálogo de Datos de Fiscalización del DOL — OSHA",
        "url": "https://data.dol.gov/",
        "description_pt": (
            "Portal de dados abertos do Departamento do Trabalho dos Estados "
            "Unidos. Publica os arquivos completos da fiscalização da OSHA, "
            "reeditados diariamente, em "
            "https://data.dol.gov/data-catalog/OSHA/<tabela>/OSHA_<tabela>.zip"
        ),
        "description_en": (
            "The United States Department of Labor open data portal. Publishes "
            "the complete OSHA enforcement files, reissued daily, at "
            "https://data.dol.gov/data-catalog/OSHA/<table>/OSHA_<table>.zip"
        ),
        "description_es": (
            "Portal de datos abiertos del Departamento de Trabajo de los "
            "Estados Unidos. Publica los archivos completos de fiscalización "
            "de OSHA, reeditados diariamente, en "
            "https://data.dol.gov/data-catalog/OSHA/<tabla>/OSHA_<tabla>.zip"
        ),
    }
]

# Tags name the subject matter. Nothing that merely restates another metadata
# field: not the area (US), not the organization (OSHA), not the themes already
# attached.
#
# Held as UUIDs rather than slugs because the vocabularies are slugged in
# different languages per environment — staging is Portuguese, prod is English —
# while the ids are the same. Resolving by slug works on staging and silently
# matches nothing on prod, which is how the first registration ended up with no
# tags at all.
TAGS: list[str] = [
    "417285b0-247f-40c5-8d94-d1c5b26ecd78",  # acidente_de_trabalho / workplace_accident
    "8b9b235d-ce53-4ee2-947e-d56888da3ec9",  # seguranca / security
    "cc64207a-9aeb-4283-bebf-94f39d0c98b2",  # saude / health
    "161d4c2e-a61e-481d-8821-3f70b534c063",  # trabalho / labor
    "3ee4d3c4-0ee2-436b-bd7f-76293cbc0bf2",  # fiscalizacao / oversight
    "b9c6eff2-eeb8-4dde-b8f1-115706ec7b69",  # multa / fine
    "536be6c2-7fc6-4409-a029-fb8e1c771dec",  # empresa / firm
    "de92651d-de18-4c64-87fc-d6991463fcd1",  # risco / risk
    "73bd61b6-e0f4-4e8f-bb46-6245b00fe919",  # regulacao / regulation
    "afd5cec6-1f15-474b-a21d-9d7414441518",  # obito / death
]


#: `compute_coverage_ranges` validates the coverage id as a UUID, and the pro
#: Coverage does not exist yet when the free range is computed. Only the free
#: half of the result is used at that point.
PLACEHOLDER_UUID = "00000000-0000-0000-0000-000000000000"


def read_coverages(server, table_id: str, env: str) -> dict[bool, dict]:
    """Coverages on a table, keyed by ``is_closed``.

    ``get_dataset`` does not return ``isClosed``, and it is the whole free/pro
    discriminator, so this reads it straight from GraphQL. Without it a re-run
    cannot tell the two coverages apart and would create a third.
    """
    q = """query($id: ID!) { allTable(id: $id) { edges { node { coverages {
        edges { node { id isClosed datetimeRanges { edges { node { id } } } } }
    } } } } }"""
    edges = server._gql(q, {"id": table_id}, env=env)["allTable"]["edges"]
    if not edges:
        return {}
    out: dict[bool, dict] = {}
    for e in edges[0]["node"]["coverages"]["edges"]:
        node = e["node"]
        out[bool(node["isClosed"])] = {
            "id": server._strip_id(node["id"]),
            "datetime_ranges": [
                {"id": server._strip_id(r["node"]["id"])}
                for r in node["datetimeRanges"]["edges"]
            ],
        }
    return out


def _range_id(coverage: dict | None) -> str | None:
    """Existing DateTimeRange id on a coverage, so a re-run updates it."""
    if not coverage:
        return None
    ranges = coverage.get("datetime_ranges") or []
    return ranges[0]["id"] if ranges else None


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--env", default="staging")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    sys.path.insert(0, MCP)
    import server

    sys.path.insert(0, str(HERE.parents[2]))
    from pipelines.datasets.us_osha_enforcement.flows import (
        _COVERAGE as FLOW_COVERAGE,
    )
    from pipelines.utils.metadata.policy import (
        CoverageIds,
        compute_coverage_ranges,
    )

    env = args.env
    arch = _load("architecture_def", HERE / "architecture_def.py")
    ids = server.discover_ids(env=env, keys=["status", "theme", "tag"])
    status_published = ids["status"]["published"]
    status_under_review = ids["status"]["under_review"]

    # --- organization -------------------------------------------------------
    try:
        org = server.lookup_id(category="organization", slug=ORG_SLUG, env=env)
        org_id = org["id"]
        log.info(f"organization {ORG_SLUG} exists: {org_id}")
    except Exception:
        if args.dry_run:
            log.info(f"[dry-run] would create organization {ORG_SLUG}")
            org_id = "<new>"
        else:
            area_us = server.lookup_id(category="area", slug="us", env=env)
            org = server.create_update_organization(
                slug=ORG_SLUG,
                name_pt="Administração de Segurança e Saúde Ocupacional (OSHA)",
                name_en="Occupational Safety and Health Administration (OSHA)",
                name_es="Administración de Seguridad y Salud Ocupacional (OSHA)",
                description_pt=(
                    "Agência do Departamento do Trabalho dos Estados Unidos "
                    "responsável por fiscalizar a segurança e a saúde no "
                    "trabalho, criada pelo Occupational Safety and Health Act "
                    "de 1970."
                ),
                description_en=(
                    "The United States Department of Labor agency responsible "
                    "for enforcing workplace safety and health, created by the "
                    "Occupational Safety and Health Act of 1970."
                ),
                description_es=(
                    "Agencia del Departamento de Trabajo de los Estados Unidos "
                    "responsable de fiscalizar la seguridad y la salud "
                    "laboral, creada por la Occupational Safety and Health Act "
                    "de 1970."
                ),
                website="https://www.osha.gov/",
                area_id=area_us["id"],
                env=env,
            )
            org_id = org["id"]
            log.info(f"organization {ORG_SLUG} created: {org_id}")

    # --- tags ---------------------------------------------------------------
    known = set(ids["tag"].values())
    tag_ids = [t for t in TAGS if t in known]
    missing = [t for t in TAGS if t not in known]
    if missing:
        log.warning(f"{len(missing)} tag id(s) absent from {env}: {missing}")
    log.info(f"{len(tag_ids)} tags resolved")

    # --- dataset ------------------------------------------------------------
    existing = server.get_dataset(slug=SLUG, env=env)
    dataset_id = existing["id"] if existing.get("found") else None
    theme_ids = [
        ids["theme"]["safety"],
        ids["theme"]["economics"],
        ids["theme"]["justice"],
    ]
    if args.dry_run:
        log.info(f"[dry-run] dataset {SLUG} -> {dataset_id or 'create'}")
        log.info(
            f"[dry-run] {len(arch.TABLES)} tables, "
            f"{sum(len(t.columns) for t in arch.TABLES)} columns"
        )
        return 0

    ds = server.create_update_dataset(
        id=dataset_id,
        slug=SLUG,
        organization_ids=[org_id],
        theme_ids=theme_ids,
        tag_ids=tag_ids,
        status_id=status_under_review,
        env=env,
        **DATASET_TEXT,
    )
    dataset_id = ds["id"]
    log.info(f"dataset {SLUG}: {dataset_id}")

    # --- raw data sources ---------------------------------------------------
    #
    # One source only, and one linked per table: `client._raw_source_id`
    # resolves a table's source through a query that raises when a table has
    # two or more, and both the poll and commit tasks go through it — a table
    # with two sources cannot run a recurring pipeline at all.
    existing_sources = {
        s["name"]: s["id"]
        for s in (
            server.get_raw_data_sources(dataset_slug=SLUG, env=env) or []
        )
    }
    license_id = server.lookup_id(category="license", slug="cc0", env=env)[
        "id"
    ]
    availability_id = server.lookup_id(
        category="availability", slug="online", env=env
    )["id"]
    area_us = server.lookup_id(category="area", slug="us", env=env)["id"]
    source_ids = []
    for src in RAW_SOURCES:
        got = server.create_update_raw_data_source(
            id=existing_sources.get(src["name_pt"]),
            dataset_id=dataset_id,
            license_id=license_id,
            availability_id=availability_id,
            language_ids=[
                server.lookup_id(category="language", slug="en", env=env)["id"]
            ],
            has_structured_data=True,
            contains_api=False,
            is_free=True,
            requires_registration=False,
            env=env,
            **src,
        )
        source_ids.append(got["id"])
        log.info(f"raw data source {src['name_en']}: {got['id']}")

    # --- tables -------------------------------------------------------------
    account = server.get_authenticated_account(env=env)
    existing = server.get_dataset(slug=SLUG, env=env)
    have = existing.get("tables", {}) or {}
    state = {
        "env": env,
        "dataset_id": dataset_id,
        "org_id": org_id,
        "raw_data_source_ids": source_ids,
        "account_id": account["id"],
        "tables": {},
    }

    entity_ids = server.discover_ids(env=env, keys=["entity"])["entity"]

    for table in arch.TABLES:
        prev = have.get(table.slug, {})
        # create_update_table fails once a table has a Coverage, so the table
        # record is written before any coverage is attached to it.
        tbl = server.create_update_table(
            id=prev.get("id"),
            slug=table.slug,
            dataset_id=dataset_id,
            name_pt=table.name_pt,
            name_en=table.name_en,
            name_es=table.name_es,
            description_pt=table.description_pt,
            description_en=table.description_en,
            description_es=table.description_es,
            status_id=status_published,
            published_by_ids=[account["id"]],
            data_cleaned_by_ids=[account["id"]],
            raw_data_source_ids=source_ids,
            env=env,
        )
        table_id = tbl["id"]
        log.info(f"table {table.slug}: {table_id}")

        # observation levels
        ol_ids: dict[str, str] = {
            ol["entity_slug"]: ol["id"]
            for ol in prev.get("observation_levels", [])
        }
        for entity_slug, _cols in OBSERVATION_LEVELS[table.slug]:
            if entity_slug in ol_ids:
                continue
            ol = server.create_update_observation_level(
                table_id=table_id,
                entity_id=entity_ids[entity_slug],
                env=env,
            )
            ol_ids[entity_slug] = ol["id"]

        # columns — one bulk call carries types, FKs, units and translations
        payload = json.loads(
            (HERE / "columns_json" / f"{table.slug}.json").read_text()
        )
        server.bulk_upsert_columns(
            table_id=table_id,
            columns_json=json.dumps(payload, ensure_ascii=False),
            env=env,
        )

        # bulk_upsert_columns does not set is_partition, and does not link a
        # column to its observation level. Both need update_column, and its
        # boolean arguments default to False — so is_partition is re-passed on
        # any column that is both a partition and a level's identifier.
        by_name = {
            c["name"]: c["id"]
            for c in server.get_dataset(slug=SLUG, env=env)["tables"][
                table.slug
            ]["columns"]
        }
        link: dict[str, str] = {}
        for entity_slug, cols in OBSERVATION_LEVELS[table.slug]:
            for col in cols:
                link[col] = ol_ids[entity_slug]
        for col in table.partition:
            link.setdefault(col, "")
        for col_name, ol_id in link.items():
            if col_name not in by_name:
                log.warning(
                    f"{table.slug}.{col_name} not registered — skipped"
                )
                continue
            server.update_column(
                column_id=by_name[col_name],
                column_name=col_name,
                table_id=table_id,
                is_partition=col_name in table.partition,
                observation_level_id=ol_id or None,
                env=env,
            )

        # cloud table
        cloud_id = (prev.get("cloud_tables") or [{}])[0].get("id")
        server.create_update_cloud_table(
            id=cloud_id,
            table_id=table_id,
            gcp_project_id="basedosdados-dev"
            if env != "prod"
            else "basedosdados",
            gcp_dataset_id=DATASET_ID,
            gcp_table_id=table.slug,
            env=env,
        )

        # Coverage. Every dated table is part_bdpro, so it needs BOTH a free
        # (is_closed=False) and a pro (is_closed=True) Coverage to exist
        # *before* the pipeline runs — assert_coverage_topology raises
        # otherwise, before anything is written. The two DateTimeRanges are
        # mutually exclusive: free ends at free_end inclusive, so pro starts
        # the following period.
        spec = FLOW_COVERAGE.get(table.slug)
        start = COVERAGE_START.get(table.slug) or ()
        existing_cov = read_coverages(server, table_id, env)
        free_cov = server.create_update_coverage(
            id=(existing_cov.get(False) or {}).get("id"),
            table_id=table_id,
            area_id=area_us,
            is_closed=False,
            env=env,
        )
        if start and spec is not None:
            ranges = compute_coverage_ranges(
                spec,
                SOURCE_END[table.slug],
                CoverageIds(free=free_cov["id"], pro=PLACEHOLDER_UUID),
            )
            if ranges.free is None or ranges.pro is None:
                raise RuntimeError(
                    f"{table.slug}: compute_coverage_ranges returned no "
                    "free/pro pair — the spec is not part_bdpro"
                )
            free_range = ranges.free
            sy, sm, sd = [*list(start), None, None][:3]
            server.create_update_datetime_range(
                id=_range_id(existing_cov.get(False)),
                coverage_id=free_cov["id"],
                start_year=sy,
                start_month=sm,
                start_day=sd,
                end_year=free_range.endYear,
                end_month=free_range.endMonth,
                end_day=free_range.endDay,
                interval=1,
                is_closed=False,
                env=env,
            )
            pro_cov = server.create_update_coverage(
                id=(existing_cov.get(True) or {}).get("id"),
                table_id=table_id,
                area_id=area_us,
                is_closed=True,
                env=env,
            )
            pro_range = ranges.pro
            server.create_update_datetime_range(
                id=_range_id(existing_cov.get(True)),
                coverage_id=pro_cov["id"],
                start_year=pro_range.startYear,
                start_month=pro_range.startMonth,
                start_day=pro_range.startDay,
                end_year=pro_range.endYear,
                end_month=pro_range.endMonth,
                end_day=pro_range.endDay,
                interval=1,
                is_closed=True,
                env=env,
            )
            free_end_txt = "-".join(
                str(v)
                for v in (
                    free_range.endYear,
                    free_range.endMonth,
                    free_range.endDay,
                )
                if v is not None
            )
            pro_start_txt = "-".join(
                str(v)
                for v in (
                    pro_range.startYear,
                    pro_range.startMonth,
                    pro_range.startDay,
                )
                if v is not None
            )
            log.info(
                f"  {table.slug}: free .. {free_end_txt} | "
                f"pro {pro_start_txt} .. {pro_range.endYear}"
            )

        # table Update — when WE last refreshed, a wall clock
        upd_id = (prev.get("updates") or [{}])[0].get("id")
        server.create_update_update(
            id=upd_id,
            table_id=table_id,
            entity_id=entity_ids["week"],
            frequency=1,
            latest=TODAY,
            env=env,
        )

        state["tables"][table.slug] = {
            "id": table_id,
            "observation_levels": ol_ids,
            "columns": len(payload),
        }

    # The raw data source Update is the SOURCE's max coverage date, not a wall
    # clock. Created here rather than waiting for the first pipeline run: a run
    # with update_metadata off leaves a Poll and no source Update.
    for source_id in source_ids:
        server.create_update_update(
            raw_data_source_id=source_id,
            entity_id=entity_ids["week"],
            frequency=1,
            latest=SOURCE_MAX_DATE,
            env=env,
        )

    server.reorder_tables(
        dataset_slug=SLUG,
        table_slugs=[t.slug for t in arch.TABLES],
        env=env,
    )

    (HERE / f"backend_ids_{env}.json").write_text(json.dumps(state, indent=1))
    log.info(f"wrote {HERE / f'backend_ids_{env}.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
