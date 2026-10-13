"""Register us_ucla_voteview (backend slug `voteview`) metadata in the Data Basis backend.

Idempotent: every child record (observation level, cloud table, coverage,
datetime range, update) is read over GraphQL first and its id passed back,
because create_update_* duplicates children when called without one. Coverages
are matched by `isClosed`, never by list position.

BD Pro: rollcalls and votes get a free (is_closed=False) and a pro
(is_closed=True) Coverage and **no** datetime range. The pipeline's first prod
run writes both ranges from the data (free ends 6 months before the latest roll
call, pro starts the next day); a static range here would either coarsen the
boundary or declare the paid window free. members and parties are free and
keyed by Congress, so they get a year range; dictionary gets no coverage.

Columns come from build_metadata.py (trilingual, from the architecture CSVs).
The dataset is registered `under_review`; `--publish` flips it to `published`
(dev/staging now; prod only after merge, table-approve and verification).

Usage (from the repo root):
    PYTHONPATH=. ~/.venvs/databasis-mcp/bin/python \
        models/us_ucla_voteview/code/register_metadata.py \
        [--env dev|staging|prod] [--dry-run] [--publish]
"""

import argparse
import datetime as dt
import json
from typing import Any

import databasis_mcp.tools.metadata as bd_meta
import databasis_mcp.tools.write as bd_write

from models.us_ucla_voteview.code.build_metadata import columns

DATASET_SLUG = "voteview"
GCP_DATASET_ID = "us_ucla_voteview"
AREA_SLUG = "us"
LICENSE_SLUG = "unknown"
THEME_SLUGS = ["politics", "government"]
TAG_SLUGS = [
    "congress",
    "vote",
    "ideology",
    "political_party",
    "legislative",
    "polarization",
]
# Staging is an older snapshot whose tag vocabulary is Portuguese (same ids).
STAGING_TAG_SLUGS = {
    "congress": "congresso",
    "vote": "voto",
    "ideology": "ideologia",
    "political_party": "partido",
    "legislative": "legislativo",
    "polarization": "polarizacao",
}
FIRST_YEAR, LAST_YEAR = 1789, 2026
SOURCE_MAX_DATE = (
    "2026-09-30T00:00:00"  # latest roll call, the source's coverage
)

ORG = {
    "slug": "ucla",
    "name_pt": "Universidade da Califórnia em Los Angeles (UCLA)",
    "name_en": "University of California, Los Angeles (UCLA)",
    "name_es": "Universidad de California en Los Ángeles (UCLA)",
    "description_pt": "Universidade pública de pesquisa em Los Angeles, Califórnia, Estados Unidos.",
    "description_en": "Public research university in Los Angeles, California, United States.",
    "description_es": "Universidad pública de investigación en Los Ángeles, California, Estados Unidos.",
    "website": "https://www.ucla.edu/",
}

DATASET = {
    "name_pt": "Voteview: votações nominais do Congresso dos EUA",
    "name_en": "Voteview: US Congressional Roll-Call Votes",
    "name_es": "Voteview: votaciones nominales del Congreso de EE. UU.",
    "description_pt": (
        "O Voteview, mantido pelo Departamento de Ciência Política da UCLA, registra "
        "todas as votações nominais da Câmara dos Representantes e do Senado dos "
        "Estados Unidos desde o 1º Congresso, em 1789, e como cada membro votou em "
        "cada uma. Também publica as estimativas DW-NOMINATE da posição ideológica de "
        "cada membro. Este conjunto cobre os Congressos 1 a 119 (1789-2026): 51.064 "
        "registros de membro por Congresso, 113.568 votações nominais e 26,4 milhões "
        "de votos individuais, atualizados semanalmente."
    ),
    "description_en": (
        "Voteview, maintained by the Department of Political Science at UCLA, records "
        "every roll-call vote taken in the US House of Representatives and Senate "
        "since the 1st Congress in 1789, and how each member voted in each. It also "
        "publishes DW-NOMINATE estimates of each member's ideological position. This "
        "dataset covers Congresses 1 to 119 (1789-2026): 51,064 member-Congress "
        "records, 113,568 roll calls and 26.4 million individual votes, refreshed "
        "weekly."
    ),
    "description_es": (
        "Voteview, mantenido por el Departamento de Ciencia Política de UCLA, registra "
        "todas las votaciones nominales de la Cámara de Representantes y del Senado de "
        "Estados Unidos desde el 1.er Congreso, en 1789, y cómo votó cada miembro en "
        "cada una. También publica las estimaciones DW-NOMINATE de la posición "
        "ideológica de cada miembro. Este conjunto cubre los Congresos 1 a 119 "
        "(1789-2026): 51.064 registros de miembro por Congreso, 113.568 votaciones "
        "nominales y 26,4 millones de votos individuales, actualizados semanalmente."
    ),
}

RAW_SOURCE = {
    "name_pt": "Voteview: downloads de dados",
    "name_en": "Voteview: data downloads",
    "name_es": "Voteview: descargas de datos",
    "url": "https://voteview.com/data",
    "description_pt": (
        "Página de downloads do Voteview, com os arquivos de membros, partidos, "
        "votações nominais e votos de todos os Congressos em CSV, JSON, DTA e ORD. "
        "Os arquivos são reconstruídos a cada atualização. O Voteview não declara "
        "licença e pede que o banco seja citado."
    ),
    "description_en": (
        "Voteview download page, with the members, parties, roll calls and votes "
        "files for all Congresses in CSV, JSON, DTA and ORD. The files are rebuilt "
        "on every update. Voteview states no license and asks users to cite the "
        "database."
    ),
    "description_es": (
        "Página de descargas de Voteview, con los archivos de miembros, partidos, "
        "votaciones nominales y votos de todos los Congresos en CSV, JSON, DTA y ORD. "
        "Los archivos se reconstruyen en cada actualización. Voteview no declara "
        "licencia y pide que se cite la base de datos."
    ),
}

TABLE_ORDER = ["members", "rollcalls", "votes", "parties", "dictionary"]
TABLES = {
    "members": {
        "name_pt": "Membros",
        "name_en": "Members",
        "name_es": "Miembros",
        "description_pt": (
            "Membros do Congresso dos EUA e presidentes, uma linha por membro, câmara "
            "e Congresso, com dados biográficos e as estimativas de ponto ideal "
            "DW-NOMINATE e Nokken-Poole."
        ),
        "description_en": (
            "Members of the US Congress and presidents, one row per member, chamber "
            "and Congress, with biographical fields and DW-NOMINATE and Nokken-Poole "
            "ideal-point estimates."
        ),
        "description_es": (
            "Miembros del Congreso de EE. UU. y presidentes, una fila por miembro, "
            "cámara y Congreso, con datos biográficos y las estimaciones de punto "
            "ideal DW-NOMINATE y Nokken-Poole."
        ),
        "levels": {"legislature": "congress", "person": "icpsr"},
        "tier": "free",
    },
    "rollcalls": {
        "name_pt": "Votações nominais",
        "name_en": "Roll calls",
        "name_es": "Votaciones nominales",
        "description_pt": (
            "Votações nominais da Câmara e do Senado dos EUA, uma linha por votação, "
            "com o resultado, a contagem de votos e o ponto médio e a dispersão "
            "DW-NOMINATE."
        ),
        "description_en": (
            "Roll-call votes of the US House and Senate, one row per roll call, with "
            "the result, the vote counts and the DW-NOMINATE midpoint and spread."
        ),
        "description_es": (
            "Votaciones nominales de la Cámara y el Senado de EE. UU., una fila por "
            "votación, con el resultado, el recuento de votos y el punto medio y la "
            "dispersión DW-NOMINATE."
        ),
        "levels": {"legislature": "congress", "vote": "rollnumber"},
        "tier": "part_bdpro",
    },
    "votes": {
        "name_pt": "Votos",
        "name_en": "Votes",
        "name_es": "Votos",
        "description_pt": (
            "Como cada membro do Congresso dos EUA votou em cada votação nominal, uma "
            "linha por membro e votação, com a probabilidade NOMINATE do voto "
            "registrado."
        ),
        "description_en": (
            "How each member of the US Congress voted in each roll call, one row per "
            "member and roll call, with the NOMINATE probability of the recorded vote."
        ),
        "description_es": (
            "Cómo votó cada miembro del Congreso de EE. UU. en cada votación nominal, "
            "una fila por miembro y votación, con la probabilidad NOMINATE del voto "
            "registrado."
        ),
        "levels": {
            "legislature": "congress",
            "vote": "rollnumber",
            "person": "icpsr",
        },
        "tier": "part_bdpro",
    },
    "parties": {
        "name_pt": "Partidos",
        "name_en": "Parties",
        "name_es": "Partidos",
        "description_pt": (
            "Partidos do Congresso dos EUA, uma linha por partido, câmara e Congresso, "
            "com o número de membros e a média e a mediana DW-NOMINATE."
        ),
        "description_en": (
            "Congressional parties, one row per party, chamber and Congress, with the "
            "number of members and the mean and median DW-NOMINATE scores."
        ),
        "description_es": (
            "Partidos del Congreso de EE. UU., una fila por partido, cámara y "
            "Congreso, con el número de miembros y la media y la mediana DW-NOMINATE."
        ),
        "levels": {"legislature": "congress", "party": "party_code"},
        "tier": "free",
    },
    "dictionary": {
        "name_pt": "Dicionário",
        "name_en": "Dictionary",
        "name_es": "Diccionario",
        "description_pt": "Rótulos das colunas codificadas das tabelas do conjunto.",
        "description_en": "Labels of the coded columns of the dataset's tables.",
        "description_es": "Etiquetas de las columnas codificadas de las tablas del conjunto.",
        "levels": {},
        "tier": None,
    },
}
PARTITIONS = {"rollcalls": ["congress"], "votes": ["congress"]}

CHILDREN_QUERY = """
query($id: ID) { allTable(id: $id) { edges { node {
  coverages { edges { node { id isClosed
    datetimeRanges { edges { node { id } } } } } }
  observationLevels { edges { node { id entity { slug } } } }
  cloudTables { edges { node { id } } }
  updates { edges { node { id } } }
} } } }
"""


def edges(node: dict, key: str) -> list[dict]:
    return [e["node"] for e in node[key]["edges"]]


def children(table_id: str, env: str) -> dict:
    """Existing child records of a table, ids stripped, coverages keyed by tier."""
    data = bd_meta._gql(CHILDREN_QUERY, {"id": table_id}, env=env)["allTable"][
        "edges"
    ]
    node = data[0]["node"]
    sid = bd_meta._strip_id
    covs = {}
    for c in edges(node, "coverages"):
        tier = "pro" if c["isClosed"] else "free"
        if tier in covs:
            raise RuntimeError(
                f"table {table_id}: two {tier} coverages, fix by hand"
            )
        covs[tier] = {
            "id": sid(c["id"]),
            "ranges": [sid(r["id"]) for r in edges(c, "datetimeRanges")],
        }
    return {
        "coverages": covs,
        "ols": {
            o["entity"]["slug"]: sid(o["id"])
            for o in edges(node, "observationLevels")
        },
        "cloud_tables": [sid(c["id"]) for c in edges(node, "cloudTables")],
        "updates": [sid(u["id"]) for u in edges(node, "updates")],
    }


def lookup(category: str, slug: str, env: str) -> str | None:
    try:
        return bd_meta.lookup_id(category=category, slug=slug, env=env)["id"]
    except Exception:
        return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default="dev", choices=["dev", "staging", "prod"])
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--publish", action="store_true")
    args = ap.parse_args()
    env = args.env
    gcp_project = "basedosdados" if env == "prod" else "basedosdados-dev"

    payloads = {
        t: columns(t) for t in TABLE_ORDER
    }  # validates every translation
    ids = bd_meta.discover_ids(
        env=env,
        keys=[
            "status",
            "entity",
            "license",
            "availability",
            "theme",
            "language",
        ],
    )
    status_review, status_published = (
        ids["status"]["under_review"],
        ids["status"]["published"],
    )
    theme_ids = [ids["theme"][t] for t in THEME_SLUGS]
    tag_ids = [
        bd_meta.lookup_id(
            category="tag",
            slug=STAGING_TAG_SLUGS[t] if env == "staging" else t,
            env=env,
        )["id"]
        for t in TAG_SLUGS
    ]
    area_id = bd_meta.lookup_id(category="area", slug=AREA_SLUG, env=env)["id"]
    lang = ids["language"]
    english = lang.get("en") or lang.get("english") or lang.get("ingles")
    entity = ids["entity"]
    account_id = bd_meta.get_authenticated_account(env=env)["id"]
    org_id = lookup("organization", ORG["slug"], env)
    print(
        f"env={env} org={org_id or 'TO CREATE'} themes={len(theme_ids)} tags={len(tag_ids)} "
        f"area={area_id} english={english} columns={ {t: len(p) for t, p in payloads.items()} }"
    )
    if args.dry_run:
        return

    if not org_id:
        org_id = bd_write.create_update_organization(
            **ORG, area_id=area_id, env=env
        )["id"]
        print(f"organization ucla created -> {org_id}")

    existing = bd_meta.get_dataset(slug=DATASET_SLUG, env=env)
    ds = bd_write.create_update_dataset(
        slug=DATASET_SLUG,
        **DATASET,
        organization_ids=[org_id],
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
        for s in bd_write.get_raw_data_sources(
            dataset_slug=DATASET_SLUG, env=env
        )
        if s.get("url")
    }
    source = bd_write.create_update_raw_data_source(
        dataset_id=dataset_id,
        **RAW_SOURCE,
        license_id=ids["license"][LICENSE_SLUG],
        availability_id=ids["availability"]["online"],
        has_structured_data=True,
        is_free=True,
        contains_api=False,
        requires_registration=False,
        language_ids=[english] if english else None,
        status_id=status_published,
        id=prior_sources.get(RAW_SOURCE["url"]),
        env=env,
    )
    source_id = source["id"]
    print(f"raw source -> {source_id}")

    # The source's Update is its coverage date (latest roll call), not today.
    src_updates = [
        u
        for u in bd_meta._gql(
            "query($id: ID) { allRawdatasource(id: $id) { edges { node { updates { edges { node { id } } } } } } }",
            {"id": source_id},
            env=env,
        )["allRawdatasource"]["edges"][0]["node"]["updates"]["edges"]
    ]
    bd_write.create_update_update(
        entity_id=entity["month"],
        frequency=1,
        latest=SOURCE_MAX_DATE,
        raw_data_source_id=source_id,
        id=bd_meta._strip_id(src_updates[0]["node"]["id"])
        if src_updates
        else None,
        env=env,
    )

    # A table Update is a wall clock; a naive value would be read as Sao Paulo time.
    now = dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat()
    for table in TABLE_ORDER:
        spec = TABLES[table]
        prior = (
            bd_meta.get_dataset(slug=DATASET_SLUG, env=env)
            .get("tables", {})
            .get(table, {})
        )
        common: dict[str, Any] = {
            "slug": table,
            **{
                k: spec[k]
                for k in (
                    "name_pt",
                    "name_en",
                    "name_es",
                    "description_pt",
                    "description_en",
                    "description_es",
                )
            },
            "dataset_id": dataset_id,
            "status_id": status_published,
            "published_by_ids": [account_id],
            "data_cleaned_by_ids": [account_id],
            "env": env,
        }
        table_id = bd_write.create_update_table(**common, id=prior.get("id"))[
            "id"
        ]
        kids = children(table_id, env)
        print(f"\ntable {table} -> {table_id}")

        ol_ids = {}
        for ent in spec["levels"]:
            ol_ids[ent] = bd_write.create_update_observation_level(
                table_id=table_id,
                entity_id=entity[ent],
                id=kids["ols"].get(ent),
                env=env,
            )["id"]
        if ol_ids:
            bd_write.reorder_observation_levels(
                table_id=table_id,
                ol_ids=[ol_ids[e] for e in spec["levels"]],
                env=env,
            )

        res = bd_write.bulk_upsert_columns(
            table_id=table_id,
            columns_json=json.dumps(payloads[table], ensure_ascii=False),
            env=env,
        )
        print(
            f"  columns: created={res['created']} updated={res['updated']} errors={res['errors']}"
        )
        if res["errors"]:
            raise RuntimeError(
                f"{table}: column upsert errors {res['errors']}"
            )
        bd_write.reorder_columns(
            table_id=table_id,
            column_names=[c["name"] for c in payloads[table]],
            env=env,
        )

        # Observation-level links and partition flags in one call per column:
        # update_column's booleans default to False, so separate calls clobber.
        cols = {
            c["name"]: c["id"]
            for c in bd_meta.get_dataset(slug=DATASET_SLUG, env=env)["tables"][
                table
            ]["columns"]
        }
        level_of = {col: ent for ent, col in spec["levels"].items()}
        parts = PARTITIONS.get(table, [])
        by_name = {c["name"]: c for c in payloads[table]}
        for col in sorted(set(level_of) | set(parts)):
            c = by_name[col]
            bd_write.update_column(
                column_id=cols[col],
                column_name=col,
                table_id=table_id,
                is_partition=col in parts,
                covered_by_dictionary=c["covered_by_dictionary"],
                has_sensitive_data=c["has_sensitive_data"],
                measurement_unit=c["measurement_unit"],
                directory_column_name=c["directory_column"],
                description_pt=c["description_pt"],
                description_en=c["description_en"],
                description_es=c["description_es"],
                observations_pt=c["observations_pt"],
                observations_en=c["observations_en"],
                observations_es=c["observations_es"],
                observation_level_id=ol_ids.get(level_of.get(col)),
                env=env,
            )
        print(f"  flagged {sorted(set(level_of) | set(parts))}")

        bd_write.create_update_cloud_table(
            table_id=table_id,
            gcp_project_id=gcp_project,
            gcp_dataset_id=GCP_DATASET_ID,
            gcp_table_id=table,
            id=kids["cloud_tables"][0] if kids["cloud_tables"] else None,
            env=env,
        )

        covs = kids["coverages"]
        if spec["tier"] == "free":
            cov = bd_write.create_update_coverage(
                table_id=table_id, area_id=area_id, is_closed=False,
                id=covs.get("free", {}).get("id"), env=env,
            )  # fmt: skip
            ranges = covs.get("free", {}).get("ranges", [])
            bd_write.create_update_datetime_range(
                coverage_id=cov["id"], start_year=FIRST_YEAR, end_year=LAST_YEAR,
                interval=1, id=ranges[0] if ranges else None, env=env,
            )  # fmt: skip
            print(f"  free coverage {FIRST_YEAR}(1){LAST_YEAR}")
        elif spec["tier"] == "part_bdpro":
            for tier, closed in (("free", False), ("pro", True)):
                bd_write.create_update_coverage(
                    table_id=table_id, area_id=area_id, is_closed=closed,
                    id=covs.get(tier, {}).get("id"), env=env,
                )  # fmt: skip
            print("  free + pro coverages, ranges left to the pipeline")

        bd_write.create_update_update(
            entity_id=entity["week"],
            frequency=1,
            latest=now,
            table_id=table_id,
            id=kids["updates"][0] if kids["updates"] else None,
            env=env,
        )
        bd_write.create_update_table(
            **common, raw_data_source_ids=[source_id], id=table_id
        )
        print("  cloud table, update and raw source linked")

    bd_write.reorder_tables(
        dataset_slug=DATASET_SLUG, table_slugs=TABLE_ORDER, env=env
    )
    print(f"\n=== METADATA REGISTRATION COMPLETE (env={env}) ===")


if __name__ == "__main__":
    main()
