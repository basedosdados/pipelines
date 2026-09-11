"""Register the dataset's tables, columns, observation levels and coverage.

Runs against the staging backend. The column payloads come from
``metadata_payload.py``, which generates them from the schema module, so the
backend records cannot drift from the architecture CSVs or the dbt models.

Idempotent: every record is looked up by name before it is written, and the
existing id is passed back so a re-run updates rather than duplicating. The
create_update_* tools duplicate silently when no id is supplied.

Usage::

    PYTHONPATH=. python models/au_nsw_nswec_elections/code/register_metadata.py
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
DATASET_SLUG = "nsw_elections"
DATASET_ID = "1da63b84-e288-4912-864b-f43c931fffae"
GCP_PROJECT = "basedosdados-dev"
GCP_DATASET = "au_nsw_nswec_elections"
AREA_AU_NSW = "fc1387e0-6311-47c9-bc2b-faff353425c4"
ACCOUNT = "57"
STATUS_PUBLISHED = "e16221de-ac30-4926-83d3-de219998dab3"

META = pathlib.Path(__file__).resolve().parent / "metadata"

RAW_SOURCES = {
    2011: "462440f7-3bdb-493b-b5a6-af171e50bfa8",
    2015: "c4924204-7485-4340-bc0f-9bebaa5051af",
    2019: "6f2606db-b56b-4501-b6ab-e71798d28dcb",
    2023: "d839d97f-bafa-4bbb-b79a-b342ca69bb28",
}

ENTITY = {
    "year": "e1bf146e-b6bb-4b65-bee7-c800876e80a5",
    "election": "1cf89a2d-6fd0-44af-a115-dc10dc5f0cb5",
    "district": "031c0045-f144-4aea-b294-dcf91d0ac9c8",
    "person": "b4e76213-888b-40ea-b877-d82ce76d71a2",
    "electoral_booth": "74ad2b0b-bba1-43b1-ac10-8cea360f6515",
    "vote": "7bb94a71-12b8-42d2-b881-e750718a534f",
}

# Table order on the dataset page: the catalogue first, then the results from
# coarsest to finest grain, then the reference tables.
TABLE_ORDER = [
    "election",
    "candidate",
    "result_district",
    "result_voting_centre",
    "distribution_of_preferences",
    "ballot_preference",
    "enrolment_turnout",
    "voting_centre",
    "dicionario",
]

# Observation levels per table, mirroring au_qld_ecq_elections, mapped to the
# column that identifies each level. Order matters: coarsest first.
OBSERVATION_LEVELS: dict[str, list[tuple[str, str | None]]] = {
    "election": [("year", "year"), ("election", "election_id")],
    "candidate": [
        ("year", "year"),
        ("election", "election_id"),
        ("district", "contest_id"),
        ("person", "ballot_name"),
    ],
    "result_district": [
        ("year", "year"),
        ("election", "election_id"),
        ("district", "contest_id"),
        ("person", "ballot_name"),
    ],
    "result_voting_centre": [
        ("year", "year"),
        ("election", "election_id"),
        ("district", "voting_centre_district_name"),
        ("electoral_booth", "voting_centre_name"),
        ("person", "ballot_name"),
    ],
    "distribution_of_preferences": [
        ("year", "year"),
        ("election", "election_id"),
        ("district", "contest_id"),
        ("person", "ballot_name"),
    ],
    "ballot_preference": [
        ("year", "year"),
        ("election", "election_id"),
        ("district", "contest_id"),
        ("electoral_booth", "voting_centre_name"),
        ("vote", "ballot_paper_id"),
    ],
    "enrolment_turnout": [
        ("year", "year"),
        ("election", "election_id"),
        ("district", "contest_id"),
    ],
    "voting_centre": [
        ("year", "year"),
        ("election", "election_id"),
        ("district", "district_name"),
        ("electoral_booth", "voting_centre_name"),
    ],
    "dicionario": [],
}

# 2011 published no voting-centre results, no distribution of preferences, no
# ballot-level data and no enrolment, so those tables carry only the three
# events that do.
SOURCES_2015_ON = [RAW_SOURCES[y] for y in (2015, 2019, 2023)]
ALL_SOURCES = [RAW_SOURCES[y] for y in (2011, 2015, 2019, 2023)]
TABLE_SOURCES = {
    "election": ALL_SOURCES,
    "candidate": ALL_SOURCES,
    "result_district": ALL_SOURCES,
    "dicionario": ALL_SOURCES,
    "result_voting_centre": SOURCES_2015_ON,
    "distribution_of_preferences": SOURCES_2015_ON,
    "ballot_preference": SOURCES_2015_ON,
    "enrolment_turnout": SOURCES_2015_ON,
    "voting_centre": SOURCES_2015_ON,
}

COVERAGE = {
    "election": (2011, 2023),
    "candidate": (2011, 2023),
    "result_district": (2011, 2023),
    "dicionario": (2011, 2023),
    "result_voting_centre": (2015, 2023),
    "distribution_of_preferences": (2015, 2023),
    "ballot_preference": (2015, 2023),
    "enrolment_turnout": (2015, 2023),
    "voting_centre": (2015, 2023),
}

# When the tables were last refreshed at Data Basis — a wall clock, not a
# coverage date. The elections are finished events, so the cadence is the four
# year term of the Legislative Assembly.
LAST_REFRESHED = "2026-09-10T00:00:00"


def call(tool, /, **kwargs):
    return getattr(tool, "fn", tool)(**kwargs)


def log(message: str) -> None:
    print(message, flush=True)


def main() -> int:
    tables_meta = json.loads((META / "tables.json").read_text())
    existing = call(server.get_dataset, slug=DATASET_SLUG, env=ENV)
    known = existing.get("tables", {}) or {}
    table_ids: dict[str, str] = {}

    for table in TABLE_ORDER:
        meta = tables_meta[table]
        prior = known.get(table) or {}
        table_id = call(
            server.create_update_table,
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
            id=prior.get("id"),
            env=ENV,
        )["id"]
        table_ids[table] = table_id
        log(f"  table {table:30s} {table_id}")

        ol_ids: dict[str, str] = {}
        for entity_slug, _column in OBSERVATION_LEVELS[table]:
            ol_ids[entity_slug] = call(
                server.create_update_observation_level,
                table_id=table_id,
                entity_id=ENTITY[entity_slug],
                env=ENV,
            )["id"]
        if ol_ids:
            call(
                server.reorder_observation_levels,
                table_id=table_id,
                ol_ids=[ol_ids[e] for e, _ in OBSERVATION_LEVELS[table]],
                env=ENV,
            )

        payload = (META / f"{table}.json").read_text()
        result = call(
            server.bulk_upsert_columns,
            table_id=table_id,
            columns_json=payload,
            env=ENV,
        )
        log(f"    columns: {json.dumps(result)[:200]}")

        # bulk_upsert_columns does not link observation levels, so each grain
        # column is linked in its own update_column call. The boolean arguments
        # default to False, so is_partition has to be re-passed for year or the
        # bulk upsert's value is clobbered.
        column_ids = {
            c["name"]: server._strip_id(c["id"])
            for c in server._fetch_table_columns(table_id, ENV)
        }
        for entity_slug, column_name in OBSERVATION_LEVELS[table]:
            if column_name is None or column_name not in column_ids:
                continue
            call(
                server.update_column,
                column_id=column_ids[column_name],
                column_name=column_name,
                table_id=table_id,
                observation_level_id=ol_ids[entity_slug],
                is_partition=(column_name == "year" and table != "dicionario"),
                env=ENV,
            )
        if table != "dicionario":
            year_id = column_ids.get("year")
            if year_id:
                call(
                    server.update_column,
                    column_id=year_id,
                    column_name="year",
                    table_id=table_id,
                    observation_level_id=ol_ids.get("year"),
                    is_partition=True,
                    env=ENV,
                )

        call(
            server.create_update_cloud_table,
            table_id=table_id,
            gcp_project_id=GCP_PROJECT,
            gcp_dataset_id=GCP_DATASET,
            gcp_table_id=table,
            env=ENV,
        )
        coverage_id = call(
            server.create_update_coverage,
            table_id=table_id,
            area_id=AREA_AU_NSW,
            env=ENV,
        )["id"]
        start, end = COVERAGE[table]
        call(
            server.create_update_datetime_range,
            coverage_id=coverage_id,
            start_year=start,
            end_year=end,
            interval=1,
            env=ENV,
        )
        call(
            server.create_update_update,
            entity_id=ENTITY["year"],
            frequency=4,
            latest=LAST_REFRESHED,
            table_id=table_id,
            env=ENV,
        )
        log(f"    cloud table, coverage {start}-{end}, update: ok")

    # Deferred: link the raw data sources once every table exists.
    for table in TABLE_ORDER:
        meta = tables_meta[table]
        call(
            server.create_update_table,
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
            raw_data_source_ids=TABLE_SOURCES[table],
            id=table_ids[table],
            env=ENV,
        )
        log(f"  linked {len(TABLE_SOURCES[table])} raw sources to {table}")

    call(
        server.reorder_tables,
        dataset_slug=DATASET_SLUG,
        table_slugs=TABLE_ORDER,
        env=ENV,
    )
    log("  table order set")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
