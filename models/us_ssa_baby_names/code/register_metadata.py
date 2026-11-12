"""Register us_ssa_baby_names metadata in the Data Basis backend.

    python models/us_ssa_baby_names/code/register_metadata.py --env staging
    python models/us_ssa_baby_names/code/register_metadata.py --env prod
    python models/us_ssa_baby_names/code/register_metadata.py --env prod --publish

Run with an interpreter that imports ``databasis_mcp`` (``~/.venvs/databasis-mcp``)
and with the repo root on ``PYTHONPATH``. The last year of coverage is read from
the cleaned output (``$US_SSA_BABY_NAMES_DATA_DIR/output/first_name/year=*``).

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
import os
from pathlib import Path
from typing import Any

import databasis_mcp.tools.metadata as bd_mcp_metadata
import databasis_mcp.tools.write as bd_mcp_write

DATASET_SLUG = "baby_names"
GCP_DATASET = "us_ssa_baby_names"
ARCH = Path(__file__).resolve().parent / "architecture"
DATA_DIR = Path(
    os.environ.get(
        "US_SSA_BABY_NAMES_DATA_DIR",
        Path.home() / "Library" / "Caches" / "us_ssa_baby_names_data",
    )
)

# Resolved by id: the same tag record carries a Portuguese slug on staging and
# an English one on prod, while the uuid is identical in both.
TAG_IDS = [
    "58f93343-4899-479a-9977-de6042baa8db",  # names / nomes
    "ba8872aa-2f3a-461c-a9a4-1d868307a2dd",  # birth / nascimento
    "1a73fe6e-c1f6-4069-804b-dada6b08a32d",  # gender / genero
    "5ecfb4e8-ca3a-4a88-adf9-db58e52ae413",  # demographics / demografia
    "ac93baac-2449-47fc-9b92-80cb3c525a9d",  # identity / identidade
]
THEME_SLUGS = ["population", "culture-art"]
ORG_SLUG = "social_security_administration"
LICENSE_SLUG = "cc0"
NAME_ENTITY = "name"

DATASET = dict(
    slug=DATASET_SLUG,
    name_pt="Nomes de bebês nos Estados Unidos",
    name_en="Baby Names in the United States",
    name_es="Nombres de bebés en Estados Unidos",
    description_pt="Frequência de cada prenome entre as pessoas nascidas nos Estados "
    "Unidos, por sexo e ano de nascimento, segundo os pedidos de número de Seguro "
    "Social registrados pela Social Security Administration (SSA). A série nacional "
    "começa em 1880; a série por estado começa em 1910 para os 50 estados e o Distrito "
    "de Colúmbia e em 1998 para Porto Rico e os demais territórios. A SSA omite combinações com menos "
    "de 5 ocorrências para proteger a privacidade e acrescenta um ano a cada edição "
    "anual, publicada em maio.",
    description_en="Frequency of each first name among people born in the United "
    "States, by sex and year of birth, from Social Security number applications "
    "recorded by the Social Security Administration (SSA). The national series starts "
    "in 1880; the state series starts in 1910 for the 50 states and the District of "
    "Columbia and in 1998 for Puerto Rico and the other territories. SSA omits combinations with fewer than "
    "5 occurrences to protect privacy, and adds one year with each annual release, "
    "published in May.",
    description_es="Frecuencia de cada nombre de pila entre las personas nacidas en "
    "Estados Unidos, por sexo y año de nacimiento, según las solicitudes de número de "
    "Seguro Social registradas por la Social Security Administration (SSA). La serie "
    "nacional comienza en 1880; la serie por estado comienza en 1910 para los 50 "
    "estados y el Distrito de Columbia y en 1998 para Puerto Rico y los demás territorios. La SSA omite "
    "las combinaciones con menos de 5 ocurrencias para proteger la privacidad y añade "
    "un año con cada edición anual, publicada en mayo.",
)

SOURCE = dict(
    name_pt="Popular Baby Names — National and State Data",
    name_en="Popular Baby Names — National and State Data",
    name_es="Popular Baby Names — National and State Data",
    description_pt="Arquivos compactados da SSA com a frequência de prenomes por "
    "sexo e ano de nascimento: nacional (names.zip), por estado (namesbystate.zip) e "
    "por território (namesbyterritory.zip), cada um com a série completa e "
    "republicado a cada ano.",
    description_en="SSA archives with first-name frequencies by sex and year of "
    "birth: national (names.zip), by state (namesbystate.zip) and by territory "
    "(namesbyterritory.zip), each holding the full series and republished every year.",
    description_es="Archivos comprimidos de la SSA con la frecuencia de nombres de "
    "pila por sexo y año de nacimiento: nacional (names.zip), por estado "
    "(namesbystate.zip) y por territorio (namesbyterritory.zip), cada uno con la "
    "serie completa y republicado cada año.",
    url="https://www.ssa.gov/oact/babynames/limits.html",
)

# slug -> (names, descriptions, observation-level columns -> entity, first year)
TABLES: dict[str, dict[str, Any]] = {
    "first_name": dict(
        name_pt="Prenomes",
        name_en="First names",
        name_es="Nombres de pila",
        description_pt="Uma linha por prenome, sexo e ano de nascimento, a partir de "
        "1880, com o número de pessoas nascidas nos Estados Unidos que receberam o "
        "nome. Inclui apenas combinações com 5 ou mais ocorrências.",
        description_en="One row per first name, sex and year of birth, from 1880, "
        "with the number of people born in the United States who were given the "
        "name. Only combinations with 5 or more occurrences.",
        description_es="Una fila por nombre de pila, sexo y año de nacimiento, desde "
        "1880, con el número de personas nacidas en Estados Unidos que recibieron el "
        "nombre. Solo combinaciones con 5 o más ocurrencias.",
        ols={"name": NAME_ENTITY, "year": "year"},
        first_year=1880,
    ),
    "first_name_state": dict(
        name_pt="Prenomes por estado",
        name_en="First names by state",
        name_es="Nombres de pila por estado",
        description_pt="Uma linha por prenome, sexo, estado ou território e ano de "
        "nascimento, com o número de pessoas nascidas ali que receberam o nome. Os 50 "
        "estados e o Distrito de Colúmbia desde 1910; Porto Rico desde 1998; e, desde "
        "1998, a soma de Samoa Americana, Guam, Ilhas Marianas do Norte e Ilhas "
        "Virgens Americanas, publicada pela SSA apenas agregada (sigla TR, state_id "
        "nulo). Inclui "
        "apenas combinações com 5 ou mais ocorrências no estado, de modo que a soma "
        "dos estados é menor que o total nacional.",
        description_en="One row per first name, sex, state or territory and year of "
        "birth, with the number of people born there who were given the name. The 50 "
        "states and the District of Columbia from 1910; Puerto Rico from 1998; and, from "
        "1998, the sum of American Samoa, Guam, the Northern Mariana Islands and the "
        "U.S. Virgin Islands, which SSA publishes only combined (abbreviation TR, null "
        "state_id). "
        "Only combinations with 5 or more occurrences in the state, so the states sum "
        "to less than the national total.",
        description_es="Una fila por nombre de pila, sexo, estado o territorio y año "
        "de nacimiento, con el número de personas nacidas allí que recibieron el "
        "nombre. Los 50 estados y el Distrito de Columbia desde 1910; Puerto Rico desde "
        "1998; y, desde 1998, la suma de Samoa Americana, Guam, las Islas Marianas del "
        "Norte y las Islas Vírgenes de EE. UU., que la SSA publica solo agregada (sigla "
        "TR, state_id nulo). Solo combinaciones con 5 o más ocurrencias en el estado, "
        "por lo que la suma de los estados es menor que el total nacional.",
        ols={"state_id": "state", "name": NAME_ENTITY, "year": "year"},
        first_year=1910,
    ),
    "dictionary": dict(
        name_pt="Dicionário",
        name_en="Dictionary",
        name_es="Diccionario",
        description_pt="Dicionário de valores das colunas codificadas das tabelas do "
        "conjunto (sexo).",
        description_en="Dictionary of values for the coded columns of the dataset's "
        "tables (sex).",
        description_es="Diccionario de valores de las columnas codificadas de las "
        "tablas del conjunto (sexo).",
        ols={},
        first_year=None,
    ),
}
TEXT_KEYS = [
    "name_pt",
    "name_en",
    "name_es",
    "description_pt",
    "description_en",
    "description_es",
]


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


def last_year() -> int:
    years = [
        int(p.name.split("=", 1)[1])
        for p in (DATA_DIR / "output" / "first_name").glob("year=*")
    ]
    if not years:
        raise SystemExit("no cleaned output to read the last year from")
    return max(years)


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

    existing = tool("get_dataset")(slug=DATASET_SLUG, env=env)
    if args.publish:
        if not existing.get("found"):
            print(f"dataset {DATASET_SLUG!r} not found on {env}")
            return 1
        publish(existing, env, uid("status", "published"))
        print(f"{DATASET_SLUG} published on {env}")
        return 0

    end_year = last_year()
    print(f"=== {env} (cloud tables -> {gcp_project}), data to {end_year} ===")
    status_review = uid("status", "under_review")
    status_published = uid("status", "published")
    area_us = uid("area", "us")
    entity = {e: uid("entity", e) for e in ("year", "state", NAME_ENTITY)}
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
    src = tool("create_update_raw_data_source")(
        dataset_id=dataset_id,
        **SOURCE,
        license_id=uid("license", LICENSE_SLUG),
        availability_id=uid("availability", "online"),
        has_structured_data=True,
        is_free=True,
        contains_api=False,
        requires_registration=False,
        language_ids=[uid("language", "en")],
        env=env,
        id=bare(known.get(SOURCE["url"])),
    )
    src_id = bare(src["id"])
    print("  raw source")

    tables = tool("get_dataset")(slug=DATASET_SLUG, env=env).get("tables", {})
    for slug, spec in TABLES.items():
        table_args = dict(
            slug=slug,
            **{k: spec[k] for k in TEXT_KEYS},
            dataset_id=dataset_id,
            status_id=status_published,
            published_by_ids=[account],
            data_cleaned_by_ids=[account],
            env=env,
        )
        t = tool("create_update_table")(
            **table_args, id=bare((tables.get(slug) or {}).get("id"))
        )
        tid = bare(t["id"])
        tool("bulk_upsert_columns")(
            table_id=tid, columns_json=columns_json(slug), env=env
        )
        node = gql(TABLE_STATE_Q, {"id": tid}, env)["allTable"]["edges"][0][
            "node"
        ]
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
            gcp_table_id=slug,
            id=cts[0] if cts else None,
            env=env,
        )
        if spec["first_year"] is None:
            print(f"  table {slug:18s} columns + cloud table")
            continue
        for col, ent_slug in spec["ols"].items():
            ent = entity[ent_slug]
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
            start_year=spec["first_year"],
            end_year=end_year,
            interval=1,
            id=ranges[0] if ranges else None,
            env=env,
        )
        # Table Update: a wall clock (when Data Basis last refreshed). lag=1 year,
        # since births of year Y are released in May of Y+1.
        tool("create_update_update")(
            entity_id=entity["year"],
            frequency=1,
            lag=1,
            latest=now,
            table_id=tid,
            id=ups[0] if ups else None,
            env=env,
        )
        # Deferred raw-source link; every field re-passed (no partial updates).
        tool("create_update_table")(
            **table_args, id=tid, raw_data_source_ids=[src_id]
        )
        print(
            f"  table {slug:18s} columns, OLs, cloud table, coverage, update"
        )

    # The source Update is the source's max coverage date, not today.
    existing_update = gql(
        """query($id: ID!) { allRawdatasource(id: $id) { edges { node {
             updates { edges { node { id } } } } } } }""",
        {"id": src_id},
        env,
    )["allRawdatasource"]["edges"][0]["node"]["updates"]["edges"]
    tool("create_update_update")(
        entity_id=entity["year"],
        frequency=1,
        latest=f"{end_year}-01-01T00:00:00+00:00",
        raw_data_source_id=src_id,
        id=bare(existing_update[0]["node"]["id"]) if existing_update else None,
        env=env,
    )
    print("  source update recorded")
    print(f"\nDone on {env}. Publish separately with --publish.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
