"""Register the us_fdic_bankfind metadata in the Data Basis backend.

Run with the pyenv interpreter the databasis MCP server needs:

    ~/.pyenv/versions/3.11.6/bin/python models/us_fdic_bankfind/code/register_metadata.py

It calls the MCP server's functions in-process rather than through the MCP tool
layer.  That is the same code path, and it is what makes the 290-column
`financials` payload practical: passing it as a tool argument would mean pushing
137 KB of JSON through a single call.

Two things about the backend that this script has to work around, both of which
bit during the first run:

* `create_update_observation_level` / `_cloud_table` / `_coverage` create a NEW
  record on every call when no `id` is passed, so re-running is *not*
  idempotent for them.  `dedupe()` keeps the first of each kind.
* `get_dataset` caps a table's columns at 200, so on `financials` (290 columns)
  the partition column was missing from that listing.  Column ids come from the
  uncapped `_fetch_table_columns`, whose ids are relay globals
  ("ColumnNode:<uuid>") and need the prefix stripped.
"""

from __future__ import annotations

import csv
import datetime
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path.home() / "Dropbox/BD/mcp"))
import server

ENV = "staging"
DATASET_SLUG = "bankfind"
DATASET_ID = "1b757321-c08b-47f2-a846-75e97b19a7aa"
GCP_PROJECT = "basedosdados-dev"
GCP_DATASET = "us_fdic_bankfind"
AREA_US = "61a2c232-c649-4b41-a5a3-1467b7393e11"
PUBLISHED = "e16221de-ac30-4926-83d3-de219998dab3"
ACCOUNT = ["57"]

COMPANY = "b585c285-3ad7-4b86-9c36-6195e4760a46"
QUARTER = "7b7f7bf4-785f-4d8e-8ffd-10ef825ebf44"
SERIES = "3bfd7b42-972f-4a69-af8d-048799665d61"

SRC_INSTITUTIONS = "71af6eb6-1930-46fd-aa11-8f890e8dc273"
SRC_FINANCIALS = "8ae945f6-00aa-4ebf-a206-15f1d41a620b"

ARCH = Path(__file__).resolve().parent / "architecture_trilingual"

TABLES = {
    "institution": "816af057-10ee-4081-a4a8-d0af1bad11f6",
    "indicator": "cbf38ad0-f43f-494e-857a-bdd1e774f7c0",
    "financials": "ec03efa4-d86c-47f6-86ea-458c4814ec52",
    "financials_indicator": "9c14cbbc-9b14-482e-9b59-182cc706e941",
}
NAMES = {
    "institution": ("Instituições", "Institutions", "Instituciones"),
    "indicator": ("Rubricas", "Indicators", "Partidas"),
    "financials": (
        "Dados financeiros trimestrais",
        "Quarterly financials",
        "Datos financieros trimestrales",
    ),
    "financials_indicator": (
        "Dados financeiros trimestrais por rubrica",
        "Quarterly financials by indicator",
        "Datos financieros trimestrales por partida",
    ),
}
# exactly one raw source per table: client._raw_source_id raises on two or more,
# which would break the recurring pipeline's poll on its first run
SOURCE = {
    "institution": SRC_INSTITUTIONS,
    "indicator": SRC_FINANCIALS,
    "financials": SRC_FINANCIALS,
    "financials_indicator": SRC_FINANCIALS,
}
# the columns that identify each grain; without these links the site renders the
# observation level's columns as "Não informado"
GRAIN = {
    "institution": {COMPANY: ["cert"]},
    "indicator": {SERIES: ["indicator_id"]},
    "financials": {COMPANY: ["cert"], QUARTER: ["year", "quarter"]},
    "financials_indicator": {
        COMPANY: ["cert"],
        QUARTER: ["year", "quarter"],
        SERIES: ["indicator_id"],
    },
}
PARTITION = {"financials": "year", "financials_indicator": "year"}
TEMPORAL = {"financials", "financials_indicator"}
COVERAGE_START = (1984, 3)  # 1984Q1
COVERAGE_END = (2026, 6)  # 2026Q2


def fn(name: str):
    f = getattr(server, name)
    return getattr(f, "fn", f)


def columns_payload(table: str) -> str:
    rows = []
    with (ARCH / f"{table}.csv").open() as handle:
        for r in csv.DictReader(handle):
            entry = {
                "name": r["name"],
                "bigquery_type": r["bigquery_type"],
                "description_pt": r["description_pt"],
                "description_en": r["description_en"],
                "description_es": r["description_es"],
                "covered_by_dictionary": r["covered_by_dictionary"] == "yes",
                "has_sensitive_data": r["has_sensitive_data"] == "yes",
            }
            for key, field in (
                ("directory_column", "directory_column"),
                ("measurement_unit", "measurement_unit"),
                ("observations", "observations"),
            ):
                if r[field]:
                    entry[key] = r[field]
            rows.append(entry)
    return json.dumps(rows, ensure_ascii=False)


def table_columns(table_id: str) -> dict[str, str]:
    """Column name -> bare uuid, from the uncapped query."""
    return {
        c["name"]: c["id"].split(":")[-1]
        for c in server._fetch_table_columns(table_id, ENV)
    }


def delete(kind: str, record_id: str) -> None:
    query = f"mutation($id: UUID!) {{ Delete{kind}(id: $id) {{ errors }} }}"
    payload = server._gql(query, {"id": record_id}, env=ENV)[f"Delete{kind}"]
    if payload and payload.get("errors"):
        raise RuntimeError(f"Delete{kind} {record_id}: {payload['errors']}")


def existing(node: dict) -> dict:
    """Index a table's child records so a re-run updates instead of duplicating.

    `create_update_observation_level` / `_cloud_table` / `_coverage` /
    `_update` create a NEW record whenever `id` is omitted, so a script that
    just calls them again is not idempotent -- the first re-runs of this one
    left four coverages and eight observation levels on `financials`, and the
    backend then rejected CreateUpdateTable outright with "'TableForm' has no
    field named 'coverages_areas'".
    """
    coverages = node["coverages"]
    return {
        "levels": {
            o["entity_id"]: o["id"] for o in node["observation_levels"]
        },
        "cloud": node["cloud_tables"][0]["id"]
        if node["cloud_tables"]
        else None,
        "coverage": coverages[0]["id"] if coverages else None,
        "range": (
            coverages[0]["datetime_ranges"][0]["id"]
            if coverages and coverages[0]["datetime_ranges"]
            else None
        ),
        "updates": {u["entity_id"]: u["id"] for u in node["updates"]},
    }


def prune(node: dict) -> None:
    """Delete every duplicate child record beyond the first of each kind."""
    seen: set[str] = set()
    for level in node["observation_levels"]:
        if level["entity_id"] in seen:
            delete("ObservationLevel", level["id"])
        else:
            seen.add(level["entity_id"])
    for extra in node["cloud_tables"][1:]:
        delete("CloudTable", extra["id"])
    for extra in node["coverages"][1:]:
        delete("Coverage", extra["id"])
    seen = set()
    for upd in node["updates"]:
        if upd["entity_id"] in seen:
            delete("Update", upd["id"])
        else:
            seen.add(upd["entity_id"])


def main() -> None:
    bulk = fn("bulk_upsert_columns")
    ol_create = fn("create_update_observation_level")
    col_update = fn("update_column")
    cloud = fn("create_update_cloud_table")
    coverage = fn("create_update_coverage")
    date_range = fn("create_update_datetime_range")
    update = fn("create_update_update")
    table_upsert = fn("create_update_table")
    get_dataset = fn("get_dataset")

    # drop anything an earlier run duplicated, before reusing ids
    dataset = get_dataset(slug=DATASET_SLUG, env=ENV)
    for node in dataset["tables"].values():
        prune(node)
    dataset = get_dataset(slug=DATASET_SLUG, env=ENV)

    # the backend field is a DateTime; a bare date is rejected
    today = f"{datetime.date.today().isoformat()}T00:00:00"

    for table, table_id in TABLES.items():
        result = bulk(
            table_id=table_id,
            columns_json=columns_payload(table),
            env=ENV,
            batch_size=50,
        )
        cols = table_columns(table_id)
        have = existing(dataset["tables"][table])

        for entity_id, names in GRAIN[table].items():
            level = ol_create(
                table_id=table_id,
                entity_id=entity_id,
                id=have["levels"].get(entity_id),
                env=ENV,
            )["id"]
            for name in names:
                if name not in cols:
                    continue
                # update_column's booleans default False, so is_partition has to
                # be re-passed here or the flag is clobbered
                col_update(
                    column_id=cols[name],
                    column_name=name,
                    table_id=table_id,
                    observation_level_id=level,
                    is_partition=(PARTITION.get(table) == name),
                    env=ENV,
                )

        cloud(
            table_id=table_id,
            gcp_project_id=GCP_PROJECT,
            gcp_dataset_id=GCP_DATASET,
            gcp_table_id=table,
            id=have["cloud"],
            env=ENV,
        )
        coverage_id = coverage(
            table_id=table_id,
            area_id=AREA_US,
            is_closed=False,
            id=have["coverage"],
            env=ENV,
        )["id"]
        if table in TEMPORAL:
            # quarterly data needs month granularity: year-only would report
            # 1984..2026 for data that really spans 1984-03..2026-06
            date_range(
                coverage_id=coverage_id,
                start_year=COVERAGE_START[0],
                start_month=COVERAGE_START[1],
                end_year=COVERAGE_END[0],
                end_month=COVERAGE_END[1],
                interval=1,
                id=have["range"],
                env=ENV,
            )

        # table Update: when WE last refreshed it, a wall clock
        update(
            entity_id=QUARTER,
            frequency=1,
            lag=1,
            latest=today,
            table_id=table_id,
            id=have["updates"].get(QUARTER),
            env=ENV,
        )
        print(f"{table:<22} columns={result['source_rows']:>4} registered")

    # Raw sources are linked in a deferred second pass, per the onboarding
    # convention: every raw source has to exist first.
    for table, table_id in TABLES.items():
        pt, en, es = NAMES[table]
        # the API does no partial updates, so every required field is re-passed
        table_upsert(
            slug=table,
            name_pt=pt,
            name_en=en,
            name_es=es,
            dataset_id=DATASET_ID,
            status_id=PUBLISHED,
            published_by_ids=ACCOUNT,
            data_cleaned_by_ids=ACCOUNT,
            raw_data_source_ids=[SOURCE[table]],
            id=table_id,
            env=ENV,
        )

    # raw source Update: what the SOURCE published, i.e. its max coverage date
    update(
        entity_id=QUARTER,
        frequency=1,
        latest=f"{COVERAGE_END[0]}-06-30T00:00:00",
        raw_data_source_id=SRC_FINANCIALS,
        env=ENV,
    )

    dataset = get_dataset(slug=DATASET_SLUG, env=ENV)
    print()
    for table, node in dataset["tables"].items():
        ranges = sum(
            len(c.get("datetime_ranges", [])) for c in node["coverages"]
        )
        levels = ",".join(
            sorted(o["entity_slug"] for o in node["observation_levels"])
        )
        print(
            f"{table:<22} OLs=[{levels}] cloud={len(node['cloud_tables'])} "
            f"coverage={len(node['coverages'])} ranges={ranges} "
            f"updates={len(node['updates'])}"
        )


if __name__ == "__main__":
    main()
