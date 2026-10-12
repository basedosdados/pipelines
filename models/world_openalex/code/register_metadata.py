"""Register world_openalex metadata on the Data Basis backend.

    UV_PROJECT_ENVIRONMENT=~/.venvs/databasis-mcp uv run --no-sync \
        --directory <BD/mcp> python <repo>/models/world_openalex/code/register_metadata.py dev [--materialized]

Needs the ``databasis_mcp`` package, which the pipelines env does not carry, so
it runs from the MCP's own environment. Idempotent: every create_update_* call
reuses the id read back from get_dataset, because those tools create a second
record when called without one (coverages, ranges and observation levels
multiply otherwise).

``--materialized`` writes the table-anchored Update (``latest`` = today). Pass
it only where the BigQuery tables have actually been built: the pipeline's poll
compares the next release date against that field.

Coverage ranges come from ``coverage.json`` beside this script, written by
``verify_bigquery.py`` from the built dev tables.
"""

import csv
import json
import sys
from datetime import date
from pathlib import Path
from typing import TypedDict

import databasis_mcp.tools.metadata as md
import databasis_mcp.tools.write as wr

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2]))
from models.world_openalex.code.tables import TABLES  # noqa: E402

ARGS = sys.argv[1:]
ENV = next((a for a in ARGS if not a.startswith("-")), "dev")
MATERIALIZED = "--materialized" in ARGS
SLUG = "openalex"
GCP_DATASET = "world_openalex"
GCP_PROJECT = "basedosdados" if ENV == "prod" else "basedosdados-dev"
RELEASE = "2026-09-23"  # snapshot release loaded at onboarding

DATASET = dict(
    slug=SLUG,
    name_pt="OpenAlex",
    name_en="OpenAlex",
    name_es="OpenAlex",
    description_pt=(
        "Catálogo aberto da produção científica mundial: 476 milhões de trabalhos "
        "acadêmicos (artigos, livros, teses, conjuntos de dados, preprints), 132 "
        "milhões de autores, instituições, periódicos, editoras, financiadores e "
        "financiamentos, com a rede de 2,9 bilhões de citações entre trabalhos, "
        "afiliações, tópicos, resumos e indicadores de acesso aberto. O OpenAlex é "
        "mantido pela organização sem fins lucrativos OurResearch, sucede o "
        "Microsoft Academic Graph e agrega Crossref, PubMed, DataCite, ORCID, ROR e "
        "milhares de repositórios. A Base dos Dados reconstrói todas as tabelas a "
        "cada publicação trimestral do snapshot."
    ),
    description_en=(
        "Open catalog of the world's research output: 476 million scholarly works "
        "(articles, books, theses, datasets, preprints), 132 million authors, "
        "institutions, journals, publishers, funders and awards, with the network of "
        "2.9 billion citations between works, affiliations, topics, abstracts and "
        "open access indicators. OpenAlex is maintained by the nonprofit OurResearch, "
        "succeeds the Microsoft Academic Graph and aggregates Crossref, PubMed, "
        "DataCite, ORCID, ROR and thousands of repositories. Data Basis rebuilds "
        "every table on each quarterly snapshot release."
    ),
    description_es=(
        "Catálogo abierto de la producción científica mundial: 476 millones de "
        "trabajos académicos (artículos, libros, tesis, conjuntos de datos, "
        "preprints), 132 millones de autores, instituciones, revistas, editoriales, "
        "financiadores y subvenciones, con la red de 2.900 millones de citas entre "
        "trabajos, afiliaciones, tópicos, resúmenes e indicadores de acceso abierto. "
        "OpenAlex es mantenido por la organización sin fines de lucro OurResearch, "
        "sucede al Microsoft Academic Graph y agrega Crossref, PubMed, DataCite, "
        "ORCID, ROR y miles de repositorios. Data Basis reconstruye todas las tablas "
        "en cada publicación trimestral del snapshot."
    ),
)

ORGANIZATION = dict(
    slug="openalex",
    name_pt="OpenAlex (OurResearch)",
    name_en="OpenAlex (OurResearch)",
    name_es="OpenAlex (OurResearch)",
    description_pt="Índice aberto da produção científica mundial, mantido pela organização sem fins lucrativos OurResearch.",
    description_en="Open index of the world's research output, maintained by the nonprofit OurResearch.",
    description_es="Índice abierto de la producción científica mundial, mantenido por la organización sin fines de lucro OurResearch.",
    website="https://openalex.org",
)

THEMES = ["science-technology", "education"]
# Each tag as the spellings it carries per backend: prod's vocabulary is
# English, staging's Portuguese, for the same records.
TAGS = [
    ("research", "pesquisa"),
    ("publication", "publicacao"),
    ("academia",),
    ("citation", "citacao"),
    ("university", "universidade"),
    ("knowledge", "conhecimento"),
    ("innovation", "inovacao"),
]
# Created when missing, and flagged to the user.
NEW_TAGS = {"open-access": ("acesso aberto", "open access", "acceso abierto")}
NEW_ENTITIES = {
    "institution": (
        "establishment",
        "Instituição",
        "Institution",
        "Institución",
    ),
    "topic": (
        "science",
        "Tópico de pesquisa",
        "Research topic",
        "Tópico de investigación",
    ),
}


class RawSource(TypedDict):
    """One raw data source."""

    key: str
    url: str
    name_pt: str
    name_en: str
    name_es: str
    description_pt: str
    description_en: str
    description_es: str
    api: bool
    linked: bool


RAW_SOURCES: list[RawSource] = [
    dict(
        key="snapshot",
        url="https://help.openalex.org/download-all-data/openalex-snapshot",
        name_pt="Snapshot do OpenAlex",
        name_en="OpenAlex snapshot",
        name_es="Snapshot de OpenAlex",
        description_pt="Cópia completa do banco de dados do OpenAlex em Parquet e JSON Lines, no bucket público s3://openalex, publicada a cada trimestre. Fonte de todas as tabelas deste conjunto.",
        description_en="Full copy of the OpenAlex database in Parquet and JSON Lines, in the public s3://openalex bucket, released quarterly. Source of every table in this dataset.",
        description_es="Copia completa de la base de datos de OpenAlex en Parquet y JSON Lines, en el bucket público s3://openalex, publicada cada trimestre. Fuente de todas las tablas de este conjunto.",
        api=False,
        linked=True,
    ),
    dict(
        key="api",
        url="https://api.openalex.org",
        name_pt="API do OpenAlex",
        name_en="OpenAlex API",
        name_es="API de OpenAlex",
        description_pt="Interface de programação do OpenAlex, atualizada diariamente; exige chave e cobra por uso desde fevereiro de 2026. Não é a fonte das tabelas deste conjunto.",
        description_en="The OpenAlex programming interface, updated daily; it requires a key and bills by usage since February 2026. It is not the source of this dataset's tables.",
        description_es="Interfaz de programación de OpenAlex, actualizada diariamente; exige clave y cobra por uso desde febrero de 2026. No es la fuente de las tablas de este conjunto.",
        api=True,
        linked=False,
    ),
]

# Observation levels per table: (entity slug, column linked to it).
_W = [("article", "work_id")]
OBSERVATION_LEVELS = {
    "work": _W,
    "work_abstract": _W,
    "work_authorship": [*_W, ("person", "author_id")],
    "work_authorship_institution": [
        *_W,
        ("person", "author_sequence"),
        ("institution", "institution_id"),
    ],
    "work_authorship_affiliation": [*_W, ("person", "author_sequence")],
    "work_authorship_country": [
        *_W,
        ("person", "author_sequence"),
        ("country", "country_code"),
    ],
    "work_location": [*_W, ("journal", "source_id")],
    "work_topic": [*_W, ("topic", "topic_id")],
    "work_keyword": [*_W, ("word", "keyword_id")],
    "work_sdg": [*_W, ("code", "sdg_id")],
    "work_mesh": [*_W, ("word", "descriptor_id")],
    "work_award": [*_W, ("grant", "award_id")],
    "work_funder": [*_W, ("agency", "funder_id")],
    "work_reference": [*_W, ("citation", "referenced_work_id")],
    "work_counts_by_year": [*_W, ("year", "year")],
    "work_indexed_in": [*_W, ("dataset", "index_name")],
    "author": [("person", "author_id")],
    "author_alternative_name": [
        ("person", "author_id"),
        ("name", "alternative_name"),
    ],
    "author_affiliation": [
        ("person", "author_id"),
        ("institution", "institution_id"),
        ("year", "year"),
    ],
    "author_last_known_institution": [
        ("person", "author_id"),
        ("institution", "institution_id"),
    ],
    "author_topic": [("person", "author_id"), ("topic", "topic_id")],
    "author_counts_by_year": [("person", "author_id"), ("year", "year")],
    "award": [("grant", "award_id")],
    "award_investigator": [
        ("grant", "award_id"),
        ("person", "investigator_sequence"),
    ],
    "award_institution": [
        ("grant", "award_id"),
        ("institution", "institution_id"),
    ],
    "award_topic": [("grant", "award_id"), ("topic", "topic_id")],
    "institution": [("institution", "institution_id")],
    "institution_association": [
        ("institution", "institution_id"),
        ("institution", "associated_institution_id"),
    ],
    "source": [("journal", "source_id")],
    "source_issn": [("journal", "source_id")],
    "publisher": [("company", "publisher_id")],
    "funder": [("agency", "funder_id")],
    "topic": [("topic", "topic_id")],
    "subfield": [("topic", "subfield_id")],
    "field": [("topic", "field_id")],
    "domain": [("topic", "domain_id")],
    "keyword": [("word", "keyword_id")],
    "dicionario": [],
}


def columns_payload(table: str) -> list[dict]:
    """bulk_upsert_columns payload from the architecture CSV (empty fields omitted)."""
    out = []
    with (HERE / "architecture" / f"{table}.csv").open(encoding="utf-8") as fh:
        for c in csv.DictReader(fh):
            e = {
                "name": c["name"],
                "bigquery_type": c["bigquery_type"],
                "description_pt": c["description_pt"],
                "description_en": c["description_en"],
                "description_es": c["description_es"],
                "covered_by_dictionary": c["covered_by_dictionary"] == "yes",
                "has_sensitive_data": c["has_sensitive_data"] == "yes",
            }
            for k in (
                "directory_column",
                "measurement_unit",
                "temporal_coverage",
            ):
                if c[k]:
                    e[k] = c[k]
            if c["observations_pt"]:
                for lang in ("pt", "en", "es"):
                    e[f"observations_{lang}"] = c[f"observations_{lang}"]
            out.append(e)
    return out


def upsert_table(
    table_id: str | None,
    table: str,
    dataset_id: str,
    status_id: str,
    account: str,
    raw_ids: list[str] | None = None,
) -> str:
    """Create or update one table record; returns its id."""
    spec = TABLES[table]
    return wr.create_update_table(
        id=table_id,
        slug=table,
        dataset_id=dataset_id,
        status_id=status_id,
        published_by_ids=[account],
        data_cleaned_by_ids=[account],
        name_pt=spec["name"][0],
        name_en=spec["name"][1],
        name_es=spec["name"][2],
        description_pt=spec["description"][0],
        description_en=spec["description"][1],
        description_es=spec["description"][2],
        raw_data_source_ids=raw_ids,
        env=ENV,
    )["id"]


def entity_ids() -> dict[str, str]:
    """Existing entities, creating the ones this dataset needs."""
    entity = dict(md.discover_ids(env=ENV, keys=["entity"])["entity"])
    # discover_ids cannot read entity categories (it queries allEntityCategory;
    # the schema calls it allEntitycategory), so go through _gql.
    cats = {
        e["node"]["slug"]: md._strip_id(e["node"]["id"])
        for e in md._gql(
            "query { allEntitycategory { edges { node { id slug } } } }",
            {},
            env=ENV,
        )["allEntitycategory"]["edges"]
    }
    for slug, (cat, pt, en, es) in NEW_ENTITIES.items():
        if slug not in entity:
            entity[slug] = wr.create_update_entity(
                slug=slug,
                name_pt=pt,
                name_en=en,
                name_es=es,
                category_id=cats[cat],
                env=ENV,
            )["id"]
            print(f"NEW entity {slug} -> {entity[slug]}")
    return entity


def tag_ids() -> list[str]:
    tags = md.discover_ids(env=ENV, keys=["tag"])["tag"]
    out = []
    for alts in TAGS:
        hit = next((tags[t] for t in alts if t in tags), None)
        if hit is None:
            raise SystemExit(f"no tag for any of {alts} in {ENV}")
        out.append(hit)
    for slug, (pt, en, es) in NEW_TAGS.items():
        if slug not in tags:
            tags[slug] = wr.create_update_tag(
                slug=slug, name_pt=pt, name_en=en, name_es=es, env=ENV
            )["id"]
            print(f"NEW tag {slug} -> {tags[slug]}")
        out.append(tags[slug])
    return out


def organization_id(area_world: str) -> str:
    try:
        return md.lookup_id(
            category="organization", slug=ORGANIZATION["slug"], env=ENV
        )["id"]
    except Exception:
        oid = wr.create_update_organization(
            **ORGANIZATION, area_id=area_world, env=ENV
        )["id"]
        print(f"NEW organization {ORGANIZATION['slug']} -> {oid}")
        return oid


def main() -> int:
    print(f"env={ENV} materialized={MATERIALIZED}")
    # Absent until verify_bigquery.py has run on the built tables; the date
    # ranges are then added by a re-run, which reuses every record id.
    cov_path = HERE / "coverage.json"
    coverage = json.loads(cov_path.read_text()) if cov_path.exists() else {}
    ids = md.discover_ids(
        env=ENV, keys=["status", "license", "availability", "theme"]
    )
    account = md.get_authenticated_account(env=ENV)["id"]
    area_world = md.lookup_id(category="area", slug="world", env=ENV)["id"]
    org = organization_id(area_world)
    entity = entity_ids()
    tags = tag_ids()
    published, under_review = (
        ids["status"]["published"],
        ids["status"]["under_review"],
    )
    themes = [ids["theme"][t] for t in THEMES]

    def dataset(status: str, dataset_id: str | None) -> str:
        return wr.create_update_dataset(
            id=dataset_id,
            **DATASET,
            organization_ids=[org],
            theme_ids=themes,
            tag_ids=tags,
            status_id=status,
            env=ENV,
        )["id"]

    existing = md.get_dataset(slug=SLUG, env=ENV)
    dataset_id = dataset(under_review, existing.get("id"))
    print(f"dataset {SLUG} -> {dataset_id}")

    listed = wr.get_raw_data_sources(dataset_slug=SLUG, env=ENV)
    if isinstance(listed, dict):
        listed = listed.get("result", [])
    have = {s["url"]: s["id"] for s in listed}
    raw = {}
    for s in RAW_SOURCES:
        raw[s["key"]] = wr.create_update_raw_data_source(
            id=have.get(s["url"]),
            dataset_id=dataset_id,
            name_pt=s["name_pt"],
            name_en=s["name_en"],
            name_es=s["name_es"],
            description_pt=s["description_pt"],
            description_en=s["description_en"],
            description_es=s["description_es"],
            url=s["url"],
            license_id=ids["license"]["cc0"],
            availability_id=ids["availability"]["online"],
            has_structured_data=True,
            is_free=True,
            contains_api=s["api"],
            requires_registration=s["api"],
            env=ENV,
        )["id"]
        print(f"raw source {s['key']} -> {raw[s['key']]}")

    prior = md.get_dataset(slug=SLUG, env=ENV).get("tables", {})
    table_ids = {}
    for table, spec in TABLES.items():
        p = prior.get(table, {})
        tid = upsert_table(p.get("id"), table, dataset_id, published, account)
        table_ids[table] = tid

        # Observation levels, reusing existing records by entity.
        have_ol = {
            o.get("entity_id"): o["id"]
            for o in p.get("observation_levels", [])
        }
        ol_ids = {}
        for slug, _col in OBSERVATION_LEVELS[table]:
            eid = entity[slug]
            if eid not in ol_ids:
                ol_ids[eid] = wr.create_update_observation_level(
                    id=have_ol.get(eid), table_id=tid, entity_id=eid, env=ENV
                )["id"]

        res = wr.bulk_upsert_columns(
            table_id=tid,
            columns_json=json.dumps(
                columns_payload(table), ensure_ascii=False
            ),
            env=ENV,
        )
        if res.get("errors"):
            raise SystemExit(f"{table}: column errors {res['errors']}")

        # Partition flag and observation-level links: bulk_upsert sets neither.
        cols = {
            c["name"]: c["id"]
            for c in md.get_dataset(slug=SLUG, env=ENV)["tables"][table][
                "columns"
            ]
        }
        part = spec["partition"][0] if spec["partition"] else None
        links = {
            col: ol_ids[entity[slug]]
            for slug, col in OBSERVATION_LEVELS[table]
        }
        for col in sorted(set(links) | ({part} if part else set())):
            wr.update_column(
                column_id=cols[col],
                column_name=col,
                table_id=tid,
                observation_level_id=links.get(col),
                is_partition=(col == part),
                env=ENV,
            )

        have_ct = [c["id"] for c in p.get("cloud_tables", [])]
        wr.create_update_cloud_table(
            id=have_ct[0] if have_ct else None,
            table_id=tid,
            gcp_project_id=GCP_PROJECT,
            gcp_dataset_id=GCP_DATASET,
            gcp_table_id=table,
            env=ENV,
        )

        have_cov = [
            c for c in p.get("coverages", []) if c.get("area_slug") == "world"
        ]
        cov = wr.create_update_coverage(
            id=have_cov[0]["id"] if have_cov else None,
            table_id=tid,
            area_id=area_world,
            env=ENV,
        )["id"]
        if table in coverage:
            start, end = coverage[table]
            have_dt = (
                have_cov[0].get("datetime_ranges", []) if have_cov else []
            )
            wr.create_update_datetime_range(
                id=have_dt[0]["id"] if have_dt else None,
                coverage_id=cov,
                start_year=start,
                end_year=end,
                interval=1,
                is_closed=False,
                env=ENV,
            )

        if MATERIALIZED:
            have_up = [u["id"] for u in p.get("updates", [])]
            wr.create_update_update(
                id=have_up[0] if have_up else None,
                table_id=tid,
                entity_id=entity["quarter"],
                frequency=1,
                lag=0,
                latest=f"{date.today()}T00:00:00",
                env=ENV,
            )

        # Deferred raw-source link (one source per table).
        upsert_table(
            tid, table, dataset_id, published, account, [raw["snapshot"]]
        )
        print(
            f"table {table} -> {tid}: {len(ol_ids)} OLs, {len(cols)} columns"
        )

    # The raw-source Update (latest = RELEASE, the snapshot date loaded) is
    # created once by hand: get_raw_data_sources does not return a source's
    # Updates, so a re-run here could not reuse the id and would duplicate it.

    wr.reorder_tables(dataset_slug=SLUG, table_slugs=list(TABLES), env=ENV)
    print("table order set")

    if ENV in ("dev", "staging"):
        dataset(published, dataset_id)
        print(f"{ENV} dataset published")
    return 0


if __name__ == "__main__":
    sys.exit(main())
