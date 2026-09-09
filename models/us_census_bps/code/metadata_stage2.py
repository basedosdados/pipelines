"""Second half of the us_census_bps metadata registration.

Runs in three passes, in this order for a reason:

1. Tables, with the deferred raw-data-source link. ``CreateUpdateTable``
   fails with ``'TableForm' has no field named 'coverages_areas'`` on any
   table that already has a coverage, so every table write must happen
   before any coverage is created.
2. Observation levels, partition flags, coverage, datetime ranges and the
   refresh record.
3. Table order.

Idempotent: existing records are read back and their ids reused, because
create_update_* duplicates a record when called without one. Coverage is read
through GraphQL rather than ``get_dataset``, which does not return
``is_closed`` and so cannot tell the free coverage from the BD Pro one.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(
    0, "/Users/rdahis/Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp"
)
sys.path.insert(0, str(Path(__file__).resolve().parent))

import server
from metadata import (
    AUX_URL,
    BASE,
    COVERAGE,
    DATASET_SLUG,
    DICIONARIO_DESC,
    OBSERVATION_LEVELS,
    SOURCES,
    TABLE_NAMES,
    TABLE_ORDER,
    TODAY,
    UPDATE,
    next_period,
    table_description,
)

COVERAGE_QUERY = """
{ allTable(id: "%s") { edges { node {
    coverages { edges { node { id isClosed
      datetimeRanges { edges { node { id } } } } } }
    updates { edges { node { id } } }
    observationLevels { edges { node { id entity { slug } } } } } } } }
"""


def read_state(table_id: str, env: str) -> dict:
    """Read a table's coverages, updates and observation levels by id."""
    node = server._gql(COVERAGE_QUERY % table_id, {}, env=env)["allTable"][
        "edges"
    ][0]["node"]
    coverages = []
    for edge in node["coverages"]["edges"]:
        c = edge["node"]
        ranges = [
            server._strip_id(r["node"]["id"])
            for r in c["datetimeRanges"]["edges"]
        ]
        coverages.append(
            {
                "id": server._strip_id(c["id"]),
                "is_closed": c["isClosed"],
                "ranges": ranges,
            }
        )
    return {
        "coverages": coverages,
        "updates": [
            server._strip_id(u["node"]["id"]) for u in node["updates"]["edges"]
        ],
        "observation_levels": {
            o["node"]["entity"]["slug"]: server._strip_id(o["node"]["id"])
            for o in node["observationLevels"]["edges"]
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="staging")
    args = parser.parse_args()
    env = args.env

    ids = server.discover_ids(env=env, keys=["entity", "status"])
    entity = ids["entity"]
    published = ids["status"]["published"]
    account = server.get_authenticated_account(env=env)["id"]
    area = server.lookup_id("area", "us", env=env)["id"]
    dataset = server.get_dataset(DATASET_SLUG, env=env)
    tables = dataset["tables"]

    prior_sources = server.get_raw_data_sources(DATASET_SLUG, env=env)
    if isinstance(prior_sources, dict):
        prior_sources = prior_sources.get("raw_data_sources", [])
    by_url = {s["url"]: s["id"] for s in prior_sources if s.get("url")}
    source_by_table: dict[str, str] = {}
    for _level, (path, table_slugs, *_rest) in SOURCES.items():
        for table in table_slugs:
            source_by_table[table] = by_url[BASE + path]

    print("=== pass 1: tables and raw-source links ===")
    for table in TABLE_ORDER:
        info = tables[table]
        pt, en, es = TABLE_NAMES[table]
        desc = (
            DICIONARIO_DESC
            if table == "dicionario"
            else table_description(table)
        )
        server.create_update_table(
            slug=table,
            name_pt=pt,
            name_en=en,
            name_es=es,
            dataset_id=dataset["id"],
            status_id=published,
            published_by_ids=[account],
            data_cleaned_by_ids=[account],
            description_pt=desc,
            description_en=desc,
            description_es=desc,
            raw_data_source_ids=(
                [source_by_table[table]] if table in source_by_table else None
            ),
            auxiliary_files_url=(
                "" if table == "dicionario" else AUX_URL.format(table=table)
            ),
            id=info["id"],
            env=env,
        )
        print(f"  {table}: raw source {source_by_table.get(table, '-')}")

    print("\n=== pass 2: observation levels, coverage and updates ===")
    for table in TABLE_ORDER:
        if table == "dicionario":
            continue
        info = tables[table]
        table_id = info["id"]
        columns = {c["name"]: c["id"] for c in info["columns"]}
        state = read_state(table_id, env)
        print(f"  --- {table} ---")

        ol_ids: list[str] = []
        for entity_slug, column_name in OBSERVATION_LEVELS[table]:
            res = server.create_update_observation_level(
                table_id=table_id,
                entity_id=entity[entity_slug],
                id=state["observation_levels"].get(entity_slug),
                env=env,
            )
            ol_id = res.get("id") or state["observation_levels"][entity_slug]
            ol_ids.append(ol_id)
            server.update_column(
                column_id=columns[column_name],
                column_name=column_name,
                table_id=table_id,
                observation_level_id=ol_id,
                env=env,
            )
            print(f"    observation level {entity_slug} -> {column_name}")
        server.reorder_observation_levels(
            table_id=table_id, ol_ids=ol_ids, env=env
        )

        # Set last: update_column's booleans default to False, so the
        # partition flag has to outlive the observation-level writes above.
        server.update_column(
            column_id=columns["year"],
            column_name="year",
            table_id=table_id,
            is_partition=True,
            env=env,
        )

        start, end, free_end = COVERAGE[table]
        free_prior = next(
            (c for c in state["coverages"] if not c["is_closed"]), None
        )
        free_id = server.create_update_coverage(
            table_id=table_id,
            area_id=area,
            is_closed=False,
            id=(free_prior or {}).get("id"),
            env=env,
        ).get("id") or (free_prior or {}).get("id")
        free_last = free_end or end
        server.create_update_datetime_range(
            coverage_id=free_id,
            start_year=start[0],
            start_month=start[1],
            end_year=free_last[0],
            end_month=free_last[1],
            interval=1,
            is_closed=False,
            id=((free_prior or {}).get("ranges") or [None])[0],
            env=env,
        )
        print(f"    free coverage {start} .. {free_last}")

        if free_end is not None:
            pro_prior = next(
                (c for c in state["coverages"] if c["is_closed"]), None
            )
            pro_id = server.create_update_coverage(
                table_id=table_id,
                area_id=area,
                is_closed=True,
                id=(pro_prior or {}).get("id"),
                env=env,
            ).get("id") or (pro_prior or {}).get("id")
            pro_start = next_period(*free_end)
            server.create_update_datetime_range(
                coverage_id=pro_id,
                start_year=pro_start[0],
                start_month=pro_start[1],
                end_year=end[0],
                end_month=end[1],
                interval=1,
                is_closed=True,
                id=((pro_prior or {}).get("ranges") or [None])[0],
                env=env,
            )
            print(f"    BD Pro coverage {pro_start} .. {end}")

        cadence = "monthly" if table.endswith("_monthly") else "annual"
        entity_slug, frequency, lag = UPDATE[cadence]
        server.create_update_update(
            entity_id=entity[entity_slug],
            frequency=frequency,
            lag=lag,
            latest=TODAY,
            table_id=table_id,
            id=(state["updates"] or [None])[0],
            env=env,
        )
        print(f"    update: every {frequency} {entity_slug}, lag {lag}")

    server.reorder_tables(
        dataset_slug=DATASET_SLUG, table_slugs=TABLE_ORDER, env=env
    )
    print("\n=== pass 3: table order set ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
