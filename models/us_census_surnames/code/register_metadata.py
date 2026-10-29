"""Register us_census_surnames metadata in the Data Basis backend.

    python models/us_census_surnames/code/register_metadata.py --env staging
    python models/us_census_surnames/code/register_metadata.py --env prod
    python models/us_census_surnames/code/register_metadata.py --env prod --publish

Run with an interpreter that imports ``databasis_mcp`` (``~/.venvs/databasis-mcp``)
and with the repo root on ``PYTHONPATH``.

The dataset already exists on every backend as a link-only shell under the
legacy slug ``u_s_frequently_occurring_surnames`` (no tables, two raw sources:
one per census). It is reused and renamed rather than duplicated.

Idempotent: every record is looked up before it is written and existing ids are
reused, because ``create_update_*`` creates a duplicate when called without an
id. The dataset is created ``under_review`` and a re-run keeps its current
status; ``--publish`` flips it and does nothing else.
"""

from __future__ import annotations

import argparse
import csv
import datetime
import json
from pathlib import Path
from typing import Any

import databasis_mcp.tools.metadata as bd_mcp_metadata
import databasis_mcp.tools.write as bd_mcp_write

DATASET_SLUG = "surnames"
LEGACY_DATASET_SLUG = "u_s_frequently_occurring_surnames"
GCP_DATASET = "us_census_surnames"
ARCH = Path(__file__).resolve().parent / "architecture"

# Resolved by id: the same tag record carries a Portuguese slug on staging and
# an English one on prod, while the uuid is identical in both.
TAG_IDS = [
    "92123d63-2f1b-4d06-93d0-7b61a74b44f3",  # genealogy / genealogia
    "58f93343-4899-479a-9977-de6042baa8db",  # names / nomes
    "5ecfb4e8-ca3a-4a88-adf9-db58e52ae413",  # demographics / demografia
    "ac93baac-2449-47fc-9b92-80cb3c525a9d",  # identity / identidade
    "bd96f1ff-4442-4d0a-a280-7316a0d7708a",  # race (prod only)
]
THEME_SLUGS = ["population", "culture-art"]
ORG_SLUG = "census_bureau"
LICENSE_SLUG = "cc0"
SURNAME_ENTITY = "name"

DATASET = dict(
    slug=DATASET_SLUG,
    name_pt="Sobrenomes frequentes nos Estados Unidos",
    name_en="Frequently Occurring Surnames in the United States",
    name_es="Apellidos frecuentes en Estados Unidos",
    description_pt="Todos os sobrenomes que ocorrem 100 ou mais vezes nos censos "
    "decenais de 2000 e 2010 dos Estados Unidos, tabulados pelo Census Bureau: "
    "151.671 sobrenomes em 2000 e 162.253 em 2010. Para cada sobrenome, a lista traz "
    "o número de pessoas, a posição, a proporção por 100 mil pessoas recenseadas e a "
    "distribuição percentual por raça e origem hispânica. Percentuais que poderiam "
    "identificar pessoas foram suprimidos pela fonte e estão nulos.",
    description_en="Every surname occurring 100 or more times in the 2000 and 2010 "
    "U.S. decennial censuses, as tabulated by the Census Bureau: 151,671 surnames in "
    "2000 and 162,253 in 2010. For each surname, the list gives the number of people, "
    "the rank, the proportion per 100,000 people enumerated and the percentage "
    "distribution by race and Hispanic origin. Percentages that could identify "
    "individuals were suppressed by the source and are null.",
    description_es="Todos los apellidos que aparecen 100 o más veces en los censos "
    "decenales de 2000 y 2010 de Estados Unidos, tabulados por el Census Bureau: "
    "151.671 apellidos en 2000 y 162.253 en 2010. Para cada apellido, la lista indica "
    "el número de personas, la posición, la proporción por cada 100.000 personas "
    "censadas y la distribución porcentual por raza y origen hispano. Los "
    "porcentajes que podrían identificar a personas fueron suprimidos por la fuente "
    "y están nulos.",
)

# Keyed by the legacy shell's URLs, so its two records are updated in place.
SOURCES = {
    2000: dict(
        name_pt="Frequently Occurring Surnames from the Census 2000",
        name_en="Frequently Occurring Surnames from the Census 2000",
        name_es="Frequently Occurring Surnames from the Census 2000",
        description_pt="Lista de sobrenomes com 100 ou mais ocorrências no Censo "
        "2000, em CSV e Excel, com nota metodológica sobre a edição dos nomes.",
        description_en="List of surnames with 100 or more occurrences in Census 2000, "
        "in CSV and Excel, with a methodology note on how names were edited.",
        description_es="Lista de apellidos con 100 o más ocurrencias en el Censo "
        "2000, en CSV y Excel, con una nota metodológica sobre la edición de los "
        "nombres.",
        url="https://www.census.gov/topics/population/genealogy/data/2000_surnames.html",
    ),
    2010: dict(
        name_pt="Frequently Occurring Surnames from the 2010 Census",
        name_en="Frequently Occurring Surnames from the 2010 Census",
        name_es="Frequently Occurring Surnames from the 2010 Census",
        description_pt="Lista de sobrenomes com 100 ou mais ocorrências no Censo "
        "2010, em CSV e Excel, com nota metodológica sobre a edição dos nomes.",
        description_en="List of surnames with 100 or more occurrences in the 2010 "
        "Census, in CSV and Excel, with a methodology note on how names were edited.",
        description_es="Lista de apellidos con 100 o más ocurrencias en el Censo "
        "2010, en CSV y Excel, con una nota metodológica sobre la edición de los "
        "nombres.",
        url="https://www.census.gov/topics/population/genealogy/data/2010_surnames.html",
    ),
}
# One raw data source per table (the client's _raw_source_id raises on two). The
# 2000 source stays attached to the dataset.
TABLE_SOURCE = 2010

TABLE = dict(
    slug="surname",
    name_pt="Sobrenomes",
    name_en="Surnames",
    name_es="Apellidos",
    description_pt="Uma linha por sobrenome e ano do censo (2000 e 2010), com número "
    "de pessoas, posição, proporção por 100 mil pessoas e distribuição percentual por "
    "raça e origem hispânica. Inclui apenas sobrenomes com 100 ou mais ocorrências; "
    "a linha agregada ALL OTHER NAMES da lista de 2010 foi removida. Percentuais "
    "suprimidos pela fonte, marcados (S), estão nulos.",
    description_en="One row per surname and census year (2000 and 2010), with the "
    "number of people, rank, proportion per 100,000 people and percentage "
    "distribution by race and Hispanic origin. Only surnames with 100 or more "
    "occurrences; the aggregate ALL OTHER NAMES row of the 2010 list was removed. "
    "Percentages suppressed by the source, marked (S), are null.",
    description_es="Una fila por apellido y año del censo (2000 y 2010), con número "
    "de personas, posición, proporción por cada 100.000 personas y distribución "
    "porcentual por raza y origen hispano. Solo apellidos con 100 o más ocurrencias; "
    "se eliminó la fila agregada ALL OTHER NAMES de la lista de 2010. Los "
    "porcentajes suprimidos por la fuente, marcados (S), están nulos.",
)


def tool(name: str) -> Any:
    """Return an MCP tool's plain function, unwrapped from its FastMCP decorator."""
    fn = getattr(bd_mcp_metadata, name, None) or getattr(bd_mcp_write, name)
    return getattr(fn, "fn", fn)


def bare(identifier: str | None) -> str | None:
    """Strip the Relay ``XNode:`` prefix the API returns on every id."""
    if not identifier:
        return None
    return identifier.split(":", 1)[1] if ":" in identifier else identifier


def gql(query: str, variables: dict, env: str) -> dict:
    return bd_mcp_metadata._gql(query, variables, env=env, auth=False)


def existing_tags(tag_ids: list[str], env: str) -> list[str]:
    """Keep the tag ids that exist on ``env``; staging lacks some prod tags."""
    keep = []
    for t in tag_ids:
        found = gql(
            "query($id: ID!) { allTag(id: $id) { edges { node { id } } } }",
            {"id": t},
            env,
        )["allTag"]["edges"]
        if found:
            keep.append(t)
        else:
            print(f"  tag {t} not on {env}, skipped")
    return keep


def columns_json(table: str) -> str:
    """The bulk_upsert_columns payload for one table, in architecture order."""
    with (ARCH / f"{table}.csv").open(newline="", encoding="utf-8") as f:
        arch = list(csv.DictReader(f))
    out = []
    for r in arch:
        entry: dict[str, Any] = {
            "name": r["name"],
            "bigquery_type": r["bigquery_type"],
            "description_pt": r["description"],
            "description_en": r["description_en"],
            "description_es": r["description_es"],
            "covered_by_dictionary": r["covered_by_dictionary"] == "yes",
            "has_sensitive_data": r["has_sensitive_data"] == "yes",
        }
        if r["directory_column"]:
            entry["directory_column"] = r["directory_column"]
        if r["measurement_unit"]:
            entry["measurement_unit"] = r["measurement_unit"]
        if r["observations"]:
            entry["observations_pt"] = r["observations"]
            entry["observations_en"] = r["observations_en"]
            entry["observations_es"] = r["observations_es"]
        out.append(entry)
    return json.dumps(out, ensure_ascii=False)


DATASET_Q = """query($id: ID!) { allDataset(id: $id) { edges { node { slug namePt
  nameEn nameEs descriptionPt descriptionEn descriptionEs
  organizations { edges { node { id } } } themes { edges { node { id } } }
  tags { edges { node { id } } } status { id } } } } }"""

TABLE_STATE_Q = """query($id: ID!) { allTable(id: $id) { edges { node {
  observationLevels { edges { node { id entity { id } } } }
  coverages { edges { node { id datetimeRanges { edges { node { id } } } } } }
  updates { edges { node { id } } }
  cloudTables { edges { node { id } } }
  columns { edges { node { id name } } }
} } } }"""


def find_dataset(env: str) -> dict:
    ds = tool("get_dataset")(slug=DATASET_SLUG, env=env)
    if not ds.get("found"):
        ds = tool("get_dataset")(slug=LEGACY_DATASET_SLUG, env=env)
        if ds.get("found"):
            print(f"  reusing the legacy shell {LEGACY_DATASET_SLUG}")
    return ds


def publish(ds: dict, env: str, status_id: str) -> None:
    node = gql(DATASET_Q, {"id": ds["id"]}, env)["allDataset"]["edges"][0][
        "node"
    ]

    def ids(key: str) -> list[str | None]:
        return [bare(e["node"]["id"]) for e in node[key]["edges"]]

    # No partial updates: every required field is re-passed or it is blanked.
    tool("create_update_dataset")(
        id=ds["id"],
        slug=node["slug"],
        name_pt=node["namePt"],
        name_en=node["nameEn"],
        name_es=node["nameEs"],
        description_pt=node["descriptionPt"],
        description_en=node["descriptionEn"],
        description_es=node["descriptionEs"],
        organization_ids=ids("organizations"),
        theme_ids=ids("themes"),
        tag_ids=ids("tags"),
        status_id=status_id,
        env=env,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--env", default="staging", choices=["dev", "staging", "prod"]
    )
    parser.add_argument("--publish", action="store_true")
    args = parser.parse_args()
    env = args.env
    gcp_project = "basedosdados" if env == "prod" else "basedosdados-dev"
    now = (
        datetime.datetime.now(datetime.UTC).replace(microsecond=0).isoformat()
    )

    def uid(cat: str, slug: str) -> str:
        return tool("lookup_id")(category=cat, slug=slug, env=env)["id"]

    existing = find_dataset(env)
    if args.publish:
        if not existing.get("found"):
            print(f"dataset {DATASET_SLUG!r} not found on {env}")
            return 1
        publish(existing, env, uid("status", "published"))
        print(f"{DATASET_SLUG} published on {env}")
        return 0

    print(f"=== {env} (cloud tables -> {gcp_project}) ===")
    status_review = uid("status", "under_review")
    status_published = uid("status", "published")
    area_us = uid("area", "us")
    licence = uid("license", LICENSE_SLUG)
    availability = uid("availability", "online")
    language_en = uid("language", "en")
    year_entity = uid("entity", "year")
    surname_entity = uid("entity", SURNAME_ENTITY)
    account = tool("get_authenticated_account")(env=env)["id"]

    current_status = None
    if existing.get("found"):
        node = gql(DATASET_Q, {"id": existing["id"]}, env)["allDataset"][
            "edges"
        ]
        # The legacy link-only shells have no status at all.
        status = node[0]["node"]["status"] if node else None
        current_status = bare(status["id"]) if status else None
    dataset = tool("create_update_dataset")(
        **DATASET,
        organization_ids=[uid("organization", ORG_SLUG)],
        theme_ids=[uid("theme", s) for s in THEME_SLUGS],
        tag_ids=existing_tags(TAG_IDS, env),
        # A re-run must not unpublish: keep the current status if there is one.
        status_id=current_status or status_review,
        env=env,
        id=existing.get("id") if existing.get("found") else None,
    )
    dataset_id = bare(dataset["id"])
    print(f"  dataset {dataset['slug']} ({dataset_id})")

    known = {
        r["url"]: r["id"]
        for r in tool("get_raw_data_sources")(
            dataset_slug=DATASET_SLUG, env=env
        )
    }
    src_ids = {}
    for year, spec in SOURCES.items():
        r = tool("create_update_raw_data_source")(
            dataset_id=dataset_id,
            **spec,
            license_id=licence,
            availability_id=availability,
            has_structured_data=True,
            is_free=True,
            contains_api=False,
            requires_registration=False,
            language_ids=[language_en],
            env=env,
            id=bare(known.get(spec["url"])),
        )
        src_ids[year] = bare(r["id"])
        print(f"  raw source {year}")

    tables = tool("get_dataset")(slug=DATASET_SLUG, env=env).get("tables", {})
    table_args = dict(
        **TABLE,
        dataset_id=dataset_id,
        status_id=status_published,
        published_by_ids=[account],
        data_cleaned_by_ids=[account],
        env=env,
    )
    t = tool("create_update_table")(
        **table_args, id=bare((tables.get(TABLE["slug"]) or {}).get("id"))
    )
    tid = bare(t["id"])
    tool("bulk_upsert_columns")(
        table_id=tid, columns_json=columns_json(TABLE["slug"]), env=env
    )

    node = gql(TABLE_STATE_Q, {"id": tid}, env)["allTable"]["edges"][0]["node"]
    ols = {
        bare(e["node"]["entity"]["id"]): bare(e["node"]["id"])
        for e in node["observationLevels"]["edges"]
    }
    covs = [
        (
            bare(e["node"]["id"]),
            [
                bare(r["node"]["id"])
                for r in e["node"]["datetimeRanges"]["edges"]
            ],
        )
        for e in node["coverages"]["edges"]
    ]
    ups = [bare(e["node"]["id"]) for e in node["updates"]["edges"]]
    cts = [bare(e["node"]["id"]) for e in node["cloudTables"]["edges"]]
    cols = {
        e["node"]["name"]: bare(e["node"]["id"])
        for e in node["columns"]["edges"]
    }

    tool("create_update_cloud_table")(
        table_id=tid,
        gcp_project_id=gcp_project,
        gcp_dataset_id=GCP_DATASET,
        gcp_table_id=TABLE["slug"],
        id=cts[0] if cts else None,
        env=env,
    )
    for col, ent in [("surname", surname_entity), ("year", year_entity)]:
        oid = ols.get(ent) or bare(
            tool("create_update_observation_level")(
                table_id=tid, entity_id=ent, env=env
            )["id"]
        )
        ols[ent] = oid
        # update_column's booleans default False; re-pass is_partition.
        tool("update_column")(
            column_id=cols[col],
            column_name=col,
            table_id=tid,
            observation_level_id=oid,
            is_partition=(col == "year"),
            env=env,
        )
    cid, ranges = (
        covs[0]
        if covs
        else (
            bare(
                tool("create_update_coverage")(
                    table_id=tid, area_id=area_us, env=env
                )["id"]
            ),
            [],
        )
    )
    tool("create_update_datetime_range")(
        coverage_id=cid,
        start_year=2000,
        end_year=2010,
        interval=10,
        id=ranges[0] if ranges else None,
        env=env,
    )
    # The table Update is a wall clock: when Data Basis last refreshed it.
    tool("create_update_update")(
        entity_id=year_entity,
        frequency=10,
        latest=now,
        table_id=tid,
        id=ups[0] if ups else None,
        env=env,
    )
    # Deferred raw-source link; every field re-passed (no partial updates).
    tool("create_update_table")(
        **table_args, id=tid, raw_data_source_ids=[src_ids[TABLE_SOURCE]]
    )
    print(
        f"  table {TABLE['slug']}: columns, 2 OLs, cloud table, coverage, update"
    )
    print(f"\nDone on {env}. Publish separately with --publish.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
