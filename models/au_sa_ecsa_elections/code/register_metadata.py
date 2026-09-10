"""Register the dataset's tables, columns, observation levels and coverage.

Runs against the staging backend. The column payloads come from
``metadata_payload.py``, which generates them from the schema module, so the
backend records cannot drift from the architecture CSVs or the dbt models.

Idempotent: every record is looked up before it is written and the existing id is
passed back, because the create_update_* tools duplicate silently when no id is
supplied.

Ordering matters and is not the obvious one. ``create_update_table`` fails on any
table that already has a Coverage, so every table write — including the raw data
source link — happens before the first coverage is created, not after.

Usage::

    PYTHONPATH=. python models/au_sa_ecsa_elections/code/register_metadata.py
"""

from __future__ import annotations

import json
import pathlib
import sys

MCP = (
    pathlib.Path.home() / "Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp"
)
sys.path.insert(0, str(MCP))

import server  # noqa: E402

ENV = "staging"
DATASET_SLUG = "sa_elections"
DATASET_ID = "b2e707f6-b081-4cf8-a9d6-0435f13591f2"
GCP_PROJECT = "basedosdados-dev"
GCP_DATASET = "au_sa_ecsa_elections"
AREA_AU_SA = "5f5200ea-14d8-45b7-b2bc-937401d0198d"
ACCOUNT = "57"
STATUS_PUBLISHED = "e16221de-ac30-4926-83d3-de219998dab3"

META = pathlib.Path(__file__).resolve().parent / "metadata"

RAW_SOURCE_RESULTS = "aefd984f-9472-4023-8f91-3d689fb34509"
RAW_SOURCE_FUNDING = "cf6fd76e-3f73-4c1f-95e2-e7e53aa03013"

ENTITY = {
    "year": "e1bf146e-b6bb-4b65-bee7-c800876e80a5",
    "election": "1cf89a2d-6fd0-44af-a115-dc10dc5f0cb5",
    "district": "031c0045-f144-4aea-b294-dcf91d0ac9c8",
    "person": "b4e76213-888b-40ea-b877-d82ce76d71a2",
    "electoral_booth": "74ad2b0b-bba1-43b1-ac10-8cea360f6515",
    "other": "1b3a7364-3e76-4416-8af7-d52824da2d24",
}

# Table order on the dataset page: the catalogue first, then the results from
# coarsest to finest grain, then the reference tables.
TABLE_ORDER = [
    "election",
    "candidate",
    "result_district",
    "result_voting_centre",
    "distribution_of_preferences",
    "enrolment_turnout",
    "voting_centre",
    "disclosure_return",
    "dicionario",
]

# One raw data source per table: the client resolves a table's source through a
# query that raises outright when a table carries two.
TABLE_SOURCE = {t: RAW_SOURCE_RESULTS for t in TABLE_ORDER}
TABLE_SOURCE["disclosure_return"] = RAW_SOURCE_FUNDING

# The results cover the 2022 and 2026 events; the funding archive opens in 2015.
COVERAGE = {t: (2022, 2026) for t in TABLE_ORDER}
COVERAGE["disclosure_return"] = (2015, 2026)

# When the tables were last refreshed at Data Basis — a wall clock, not a
# coverage date. The Assembly's term is four years, which sets the cadence.
LAST_REFRESHED = "2026-09-10T00:00:00"
FREQUENCY = {t: 4 for t in TABLE_ORDER}
FREQUENCY["disclosure_return"] = 1


def log(message: str) -> None:
    print(message, flush=True)


def existing_coverage(table_id: str) -> str | None:
    """Read the table's coverage through GraphQL.

    ``get_dataset`` returns coverages without ``isClosed``, so a re-run cannot
    tell the free coverage from a BD Pro one and creates a duplicate instead.
    """
    query = (
        f'{{ allTable(id: "{table_id}") {{ edges {{ node {{ coverages {{ edges '
        "{ node { id isClosed datetimeRanges { edges { node { id } } } } } "
        "} } } } }"
    )
    payload = server._gql(query, {}, env=ENV)
    edges = payload["allTable"]["edges"]
    if not edges:
        return None
    for edge in edges[0]["node"]["coverages"]["edges"]:
        node = edge["node"]
        if not node["isClosed"]:
            return server._strip_id(node["id"])
    return None


def main() -> int:
    tables_meta = json.loads((META / "tables.json").read_text())
    existing = server.get_dataset(slug=DATASET_SLUG, env=ENV)
    known = existing.get("tables", {}) or {}
    table_ids: dict[str, str] = {}

    # Phase 1 — every table write, including the raw source link. Nothing here may
    # run after a coverage exists.
    for table in TABLE_ORDER:
        meta = tables_meta[table]
        prior = known.get(table) or {}
        table_id = server.create_update_table(
            slug=table,
            name_pt=meta["name_pt"],
            name_en=meta["name_en"],
            name_es=meta["name_es"],
            description_pt=meta["description_pt"],
            description_en=meta["description_en"],
            description_es=meta["description_es"],
            dataset_id=DATASET_ID,
            status_id=STATUS_PUBLISHED,
            published_by_ids=[ACCOUNT],
            data_cleaned_by_ids=[ACCOUNT],
            raw_data_source_ids=[TABLE_SOURCE[table]],
            id=prior.get("id"),
            env=ENV,
        )["id"]
        table_ids[table] = table_id
        log(f"  table {table:30s} {table_id}")

    # Phase 2 — everything that hangs off a table.
    for table in TABLE_ORDER:
        table_id = table_ids[table]
        meta = tables_meta[table]
        levels = [tuple(x) for x in meta["observation_levels"]]

        ol_ids: dict[str, str] = {}
        for entity_slug, _column in levels:
            ol_ids[entity_slug] = server.create_update_observation_level(
                table_id=table_id, entity_id=ENTITY[entity_slug], env=ENV
            )["id"]
        if ol_ids:
            server.reorder_observation_levels(
                table_id=table_id,
                ol_ids=[ol_ids[e] for e, _ in levels],
                env=ENV,
            )

        payload = (META / f"{table}.json").read_text()
        result = server.bulk_upsert_columns(
            table_id=table_id, columns_json=payload, env=ENV
        )
        log(f"    columns: {json.dumps(result)[:160]}")

        # bulk_upsert_columns does not link observation levels, so each grain
        # column is linked in its own update_column call. The boolean arguments
        # default to False, so is_partition has to be re-passed for year or the
        # bulk upsert's value is clobbered.
        column_ids = {
            c["name"]: server._strip_id(c["id"])
            for c in server._fetch_table_columns(table_id, ENV)
        }
        for entity_slug, column_name in levels:
            if column_name is None or column_name not in column_ids:
                continue
            server.update_column(
                column_id=column_ids[column_name],
                column_name=column_name,
                table_id=table_id,
                observation_level_id=ol_ids[entity_slug],
                is_partition=(column_name == "year" and table != "dicionario"),
                env=ENV,
            )
        if (
            table != "dicionario"
            and "year" in column_ids
            and "year" not in {c for _, c in levels}
        ):
            server.update_column(
                column_id=column_ids["year"],
                column_name="year",
                table_id=table_id,
                is_partition=True,
                env=ENV,
            )

        server.create_update_cloud_table(
            table_id=table_id,
            gcp_project_id=GCP_PROJECT,
            gcp_dataset_id=GCP_DATASET,
            gcp_table_id=table,
            env=ENV,
        )
        coverage_id = server.create_update_coverage(
            table_id=table_id,
            area_id=AREA_AU_SA,
            id=existing_coverage(table_id),
            env=ENV,
        )["id"]
        start, end = COVERAGE[table]
        server.create_update_datetime_range(
            coverage_id=coverage_id,
            start_year=start,
            end_year=end,
            interval=1,
            env=ENV,
        )
        server.create_update_update(
            entity_id=ENTITY["year"],
            frequency=FREQUENCY[table],
            latest=LAST_REFRESHED,
            table_id=table_id,
            env=ENV,
        )
        log(f"    cloud table, coverage {start}-{end}, update: ok")

    server.reorder_tables(
        dataset_slug=DATASET_SLUG, table_slugs=TABLE_ORDER, env=ENV
    )
    log("  table order set")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
