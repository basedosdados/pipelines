"""Register au_aec_elections tables, columns and coverage in the Data Basis backend.

Drives the databasis MCP server module directly (it is an importable Python module)
rather than issuing ~150 individual tool calls.

Usage:
    uv run --with fastmcp python models/au_aec_elections/code/register_metadata.py \
        --env staging --dataset-id <id> \
        --results-source-id <id> --disclosure-source-id <id>

**Idempotent by construction.** None of the backend's create_update_* calls for
observation levels, cloud tables, coverages, datetime ranges or updates are keyed on
anything but an explicit id, so calling them without one CREATES A DUPLICATE every
run — and a table with two coverages then breaks CreateUpdateTable outright
("'TableForm' has no field named 'coverages_areas'"). This script therefore reads the
existing records first, reuses their ids, and deletes any surplus.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

MCP_DIR = Path.home() / "Dropbox" / "BD" / "mcp"
sys.path.insert(0, str(MCP_DIR))

import server  # noqa: E402  — the databasis MCP module

from pipelines.datasets.au_aec_elections import schema  # noqa: E402
from pipelines.datasets.au_aec_elections.constants import (  # noqa: E402
    constants,
    data_root,
)

DATASET_ID = constants.DATASET_ID.value  # GCP dataset id
DATASET_SLUG = (
    "elections"  # backend slug; the organization carries the au_aec prefix
)

AREA_AU_SLUG = "au"
ENTITY_SLUGS = [
    "year",
    "state",
    "district",
    "electoral_booth",
    "election",
    "person",
    "party",
]

# Reference ids happen to be identical across staging and prod today, but resolving
# them by slug per environment means a divergence surfaces as a lookup failure here
# rather than as metadata silently attached to the wrong entity.
AREA_AU = ""
STATUS_PUBLISHED = ""
ENTITY: dict[str, str] = {}


def resolve_reference_ids(env: str) -> None:
    global AREA_AU, STATUS_PUBLISHED
    ids = server.discover_ids(env=env, keys=["entity", "status"])
    STATUS_PUBLISHED = ids["status"]["published"]
    for slug in ENTITY_SLUGS:
        entity_id = ids["entity"].get(slug)
        if not entity_id:
            raise SystemExit(f"entity {slug!r} not found on {env}")
        ENTITY[slug] = entity_id
    AREA_AU = server.lookup_id(env=env, slug=AREA_AU_SLUG, category="area")[
        "id"
    ]


# table -> [(entity key, the column that identifies that level)]
#
# List every entity dimension a row is broken down by, including each nested
# geographic level — the house convention, cf. au_ato_taxation_statistics
# (`individuals_postcode` = year, state, zip_code, item) and br_tse_eleicoes
# (`detalhes_votacao_municipio_zona` = year, municipality, electoral_zone, ...).
# Each level is linked to its identifying column: an unlinked level renders as
# "Não informado" on the site.
#
# Party is deliberately a level only on `party`. Everywhere else the row is a
# candidate or a place and the party is an attribute of it, not a dimension the
# table is broken down by.
OBSERVATION_LEVELS: dict[str, list[tuple[str, str]]] = {
    "election": [
        ("year", "year"),
        ("state", "state_abbreviation"),
        ("election", "election_id"),
    ],
    "polling_place": [
        ("year", "year"),
        ("state", "state_abbreviation"),
        ("district", "division_id"),
        ("electoral_booth", "polling_place_id"),
        ("election", "election_id"),
    ],
    "party": [
        ("year", "year"),
        ("state", "state_abbreviation"),
        ("election", "election_id"),
        ("party", "party_abbreviation"),
    ],
    "house_candidate": [
        ("year", "year"),
        ("state", "state_abbreviation"),
        ("district", "division_id"),
        ("election", "election_id"),
        ("person", "candidate_id"),
    ],
    "house_first_preference_division": [
        ("year", "year"),
        ("state", "state_abbreviation"),
        ("district", "division_id"),
        ("election", "election_id"),
        ("person", "candidate_id"),
    ],
    "house_first_preference_polling_place": [
        ("year", "year"),
        ("state", "state_abbreviation"),
        ("district", "division_id"),
        ("electoral_booth", "polling_place_id"),
        ("election", "election_id"),
        ("person", "candidate_id"),
    ],
    "house_two_candidate_preferred_polling_place": [
        ("year", "year"),
        ("state", "state_abbreviation"),
        ("district", "division_id"),
        ("electoral_booth", "polling_place_id"),
        ("election", "election_id"),
        ("person", "candidate_id"),
    ],
    "house_two_party_preferred_division": [
        ("year", "year"),
        ("state", "state_abbreviation"),
        ("district", "division_id"),
        ("election", "election_id"),
    ],
    "house_two_party_preferred_polling_place": [
        ("year", "year"),
        ("state", "state_abbreviation"),
        ("district", "division_id"),
        ("electoral_booth", "polling_place_id"),
        ("election", "election_id"),
    ],
    # The Senate is elected state-wide, so the state is the grain and there is no
    # division level.
    "senate_candidate": [
        ("year", "year"),
        ("state", "state_abbreviation"),
        ("election", "election_id"),
        ("person", "candidate_id"),
    ],
    "senate_first_preference_division": [
        ("year", "year"),
        ("state", "state_abbreviation"),
        ("district", "division_id"),
        ("election", "election_id"),
        ("person", "candidate_id"),
    ],
    "division_summary": [
        ("year", "year"),
        ("state", "state_abbreviation"),
        ("district", "division_id"),
        ("election", "election_id"),
    ],
    "referendum_polling_place": [
        ("year", "year"),
        ("state", "state_abbreviation"),
        ("district", "division_id"),
        ("electoral_booth", "polling_place_id"),
        ("election", "election_id"),
    ],
    # The disclosure tables carry no event identifier, only the AEC's event label.
    # Only the election returns name an electorate.
    "disclosure_donation": [("year", "year")],
    "disclosure_receipt": [("year", "year")],
    "disclosure_return_annual": [("year", "year")],
    "disclosure_election_return": [
        ("year", "year"),
        ("state", "electorate_state"),
        ("district", "electorate_name"),
    ],
    "dicionario": [],
}

RESULT_TABLES = {
    "election",
    "polling_place",
    "party",
    "house_candidate",
    "house_first_preference_division",
    "house_first_preference_polling_place",
    "house_two_candidate_preferred_polling_place",
    "house_two_party_preferred_division",
    "house_two_party_preferred_polling_place",
    "senate_candidate",
    "senate_first_preference_division",
    "division_summary",
    "referendum_polling_place",
}


# --------------------------------------------------------------------------------------
# Backend helpers the MCP module does not expose
# --------------------------------------------------------------------------------------


def delete(mutation: str, record_id: str, env: str) -> None:
    # The delete mutations take UUID!, not the ID! that the create/update ones use.
    server._gql(
        f"mutation($id: UUID!) {{ {mutation}(id: $id) {{ ok errors }} }}",
        {"id": record_id},
        env=env,
    )


def drop_duplicates(node: dict, env: str) -> dict:
    """Remove surplus coverages, cloud tables and observation levels on a table.

    This must run *before* CreateUpdateTable touches the table: a table carrying two
    coverages makes the backend's TableForm reject every update with
    "'TableForm' has no field named 'coverages_areas'", so the duplicates cannot be
    cleaned up through the normal update path once they exist.
    """
    covs = node.get("coverages", [])
    for cov in covs[1:]:
        for rng in cov.get("datetime_ranges", []):
            delete("DeleteDateTimeRange", rng["id"], env)
        delete("DeleteCoverage", cov["id"], env)
    for extra in node.get("cloud_tables", [])[1:]:
        delete("DeleteCloudTable", extra["id"], env)
    seen: set[str] = set()
    kept_ols = []
    for ol in node.get("observation_levels", []):
        if ol["entity_id"] in seen:
            delete("DeleteObservationLevel", ol["id"], env)
        else:
            seen.add(ol["entity_id"])
            kept_ols.append(ol)
    return {
        **node,
        "coverages": covs[:1],
        "cloud_tables": node.get("cloud_tables", [])[:1],
        "observation_levels": kept_ols,
    }


def table_updates(table_id: str, env: str) -> list[str]:
    data = server._gql(
        "query($id: ID!) { allUpdate(table_Id: $id) { edges { node { id } } } }",
        {"id": table_id},
        env=env,
        auth=False,
    )
    return [
        server._strip_id(e["node"]["id"]) for e in data["allUpdate"]["edges"]
    ]


def year_range(table: str) -> tuple[int, int] | None:
    """Min and max year actually present in the cleaned output."""
    path = data_root() / "output" / table
    years = [
        int(p.name.split("=", 1)[1]) for p in path.glob("year=*") if p.is_dir()
    ]
    return (min(years), max(years)) if years else None


def columns_payload(table: str) -> str:
    cols = []
    for c in schema.TABLES[table]:
        entry = {
            "name": c.name,
            "bigquery_type": c.bigquery_type,
            "description_pt": c.description,
            "description_en": c.description_en,
            "description_es": c.description_es,
            "covered_by_dictionary": c.covered_by_dictionary == "yes",
            "has_sensitive_data": c.has_sensitive_data == "yes",
        }
        if c.measurement_unit:
            entry["measurement_unit"] = c.measurement_unit
        if c.directory_column:
            entry["directory_column"] = c.directory_column
        if c.observations:
            entry["observations"] = c.observations
        cols.append(entry)
    return json.dumps(cols, ensure_ascii=False)


def table_payload(table: str, dataset_id: str, account_id: str) -> dict:
    meta = schema.TABLE_META[table]
    return {
        "slug": table,
        "name_pt": meta.name_pt,
        "name_en": meta.name_en,
        "name_es": meta.name_es,
        "description_pt": meta.description_pt,
        "description_en": meta.description_en,
        "description_es": meta.description_es,
        "dataset_id": dataset_id,
        "status_id": STATUS_PUBLISHED,
        "published_by_ids": [account_id],
        "data_cleaned_by_ids": [account_id],
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default="staging")
    ap.add_argument("--dataset-id", required=True)
    ap.add_argument("--results-source-id", required=True)
    ap.add_argument("--disclosure-source-id", required=True)
    ap.add_argument("--gcp-project", default="basedosdados-dev")
    ap.add_argument("--only", nargs="*", default=None)
    args = ap.parse_args()

    env = args.env
    resolve_reference_ids(env)
    account_id = server.get_authenticated_account(env=env)["id"]
    today = date.today().isoformat() + "T00:00:00"
    targets = args.only or constants.TABLES.value

    dataset = server.get_dataset(DATASET_SLUG, env=env)
    existing_tables = dataset.get("tables") or {}

    for table in constants.TABLES.value:
        if table not in targets:
            continue
        print(f"\n=== {table}")
        node = existing_tables.get(table) or {}
        if node:
            node = drop_duplicates(node, env)

        source_id = (
            args.results_source_id
            if table in RESULT_TABLES or table == "dicionario"
            else args.disclosure_source_id
        )

        payload = table_payload(table, args.dataset_id, account_id)
        table_id = server.create_update_table(
            id=node.get("id"), env=env, **payload
        )["id"]
        print(f"  table {table_id}")

        out = server.bulk_upsert_columns(
            table_id=table_id, columns_json=columns_payload(table), env=env
        )
        if out["errors"]:
            raise SystemExit(f"  column errors: {out['errors']}")
        print(f"  columns: {out['created']} created, {out['updated']} updated")

        # _fetch_table_columns returns raw Relay global ids; the mutation wants bare
        # UUIDs, so strip them the way bulk_upsert_columns does internally.
        column_ids = {
            c["name"]: server._strip_id(c["id"])
            for c in server._fetch_table_columns(table_id, env)
        }

        # --- observation levels: one per desired entity, surplus deleted ---
        wanted = OBSERVATION_LEVELS[table]
        by_entity: dict[str, list[str]] = {}
        for ol in node.get("observation_levels", []):
            by_entity.setdefault(ol["entity_id"], []).append(ol["id"])
        keep: set[str] = set()
        ordered_ols: list[str] = []
        for entity_key, column_name in wanted:
            entity_id = ENTITY[entity_key]
            pool = by_entity.get(entity_id, [])
            ol_id = pool[0] if pool else None
            ol_id = server.create_update_observation_level(
                table_id=table_id, entity_id=entity_id, id=ol_id, env=env
            )["id"]
            keep.add(ol_id)
            ordered_ols.append(ol_id)
            server.update_column(
                column_id=column_ids[column_name],
                column_name=column_name,
                table_id=table_id,
                observation_level_id=ol_id,
                # update_column's booleans default to False and would otherwise
                # clobber the partition flag on `year`.
                is_partition=(column_name == "year" and table != "dicionario"),
                env=env,
            )
        for ids in by_entity.values():
            for stale in ids:
                if stale not in keep:
                    delete("DeleteObservationLevel", stale, env)
        if ordered_ols:
            # Creation order is not display order; set it explicitly so the levels
            # read year -> state -> division -> polling place -> event -> person.
            server.reorder_observation_levels(
                table_id=table_id, ol_ids=ordered_ols, env=env
            )
        print(
            f"  observation levels: "
            f"{', '.join(e for e, _ in wanted) if wanted else '(none)'}"
        )

        if table != "dicionario" and not wanted:
            server.update_column(
                column_id=column_ids["year"],
                column_name="year",
                table_id=table_id,
                is_partition=True,
                env=env,
            )

        # --- cloud table ---
        cloud = node.get("cloud_tables", [])
        server.create_update_cloud_table(
            table_id=table_id,
            gcp_project_id=args.gcp_project,
            gcp_dataset_id=DATASET_ID,
            gcp_table_id=table,
            id=cloud[0]["id"] if cloud else None,
            env=env,
        )
        for extra in cloud[1:]:
            delete("DeleteCloudTable", extra["id"], env)
        print("  cloud table ok")

        if table != "dicionario":
            # --- coverage and its datetime range ---
            covs = node.get("coverages", [])
            cov_id = server.create_update_coverage(
                table_id=table_id,
                area_id=AREA_AU,
                is_closed=False,
                id=covs[0]["id"] if covs else None,
                env=env,
            )["id"]
            for extra in covs[1:]:
                delete("DeleteCoverage", extra["id"], env)

            span = year_range(table)
            if span:
                ranges = covs[0].get("datetime_ranges", []) if covs else []
                server.create_update_datetime_range(
                    coverage_id=cov_id,
                    start_year=span[0],
                    end_year=span[1],
                    interval=1,
                    is_closed=False,
                    id=ranges[0]["id"] if ranges else None,
                    env=env,
                )
                for extra in ranges[1:]:
                    delete("DeleteDateTimeRange", extra["id"], env)
                print(f"  coverage {span[0]}-{span[1]}")

            # --- update record ---
            # Results republish per electoral event (~3 years); the Transparency
            # Register publishes annually.
            ups = table_updates(table_id, env)
            server.create_update_update(
                table_id=table_id,
                entity_id=ENTITY["year"],
                frequency=3 if table in RESULT_TABLES else 1,
                latest=today,
                id=ups[0] if ups else None,
                env=env,
            )
            for extra in ups[1:]:
                delete("DeleteUpdate", extra, env)
            print("  update record ok")

        # Link the raw data source last, re-passing every required field.
        server.create_update_table(
            id=table_id, raw_data_source_ids=[source_id], env=env, **payload
        )
        print("  raw data source linked")

    print("\ndone.")


if __name__ == "__main__":
    main()
