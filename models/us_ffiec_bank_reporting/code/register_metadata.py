"""Register the us_ffiec_bank_reporting metadata in the Data Basis backend.

    ~/.venvs/bd-pipelines/bin/python models/us_ffiec_bank_reporting/code/register_metadata.py [staging|prod] [under_review|published]

Everything is resolved by slug at runtime, because reference ids differ between
backends, and the whole script is idempotent: re-running it updates rather than
duplicating, and a second run is a no-op.

It calls the databasis MCP server's functions in-process rather than through the
MCP tool layer -- same code path, but it makes the column payloads practical as
a single call instead of a large JSON tool argument.

Three backend behaviours this works around, each carried over from the
us_fdic_bankfind onboarding:

* **`create_update_*` is not idempotent for a table's child records.**
  Observation levels, cloud tables, coverages and updates get a brand-new row
  whenever `id` is omitted. `prune()` clears duplicates and existing ids are
  read back and passed.
* **Duplicate coverages then break `create_update_table`** with
  `'TableForm' has no field named 'coverages_areas'`, an error naming nothing
  relevant. Raw sources are therefore linked in a deferred second pass.
* **`get_dataset` caps a table's columns at 200.** Column ids come from
  `_fetch_table_columns`, whose ids are relay globals (`ColumnNode:<uuid>`) and
  need the prefix stripped.
"""

from __future__ import annotations

import csv
import datetime
import json
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path.home() / "Dropbox/BD/mcp"))
import server  # pyrefly: ignore [missing-import]  (resolved via sys.path above)
from meta_config import (
    COVERAGE,
    DATASET_DESCRIPTION,
    DATASET_NAME,
    DATASET_SLUG,
    DESCRIPTIONS,
    GCP_DATASET,
    GRAIN,
    NAMES,
    NEW_TAGS,
    ORGANIZATION,
    PARTITION,
    SOURCE_LATEST,
    SOURCES,
    TABLE_ORDER,
    TABLE_SOURCE,
    TAGS,
    UPDATE_ENTITY,
)

# the staging backend is paired with the dev GCP project; prod with prod
GCP_PROJECT = {
    "staging": "basedosdados-dev",
    "dev": "basedosdados-dev",
    "prod": "basedosdados",
}

ARCH = Path(__file__).resolve().parent / "architecture_trilingual"

ENTITIES = ("company", "quarter", "year", "series", "county", "census_tract")


def fn(name: str) -> Callable[..., Any]:
    """The plain function behind an MCP tool.

    FastMCP's decorator keeps the original callable on `.fn`; annotating the
    return type keeps every call site type-checkable, since `getattr` alone is
    `Any | None` to the checker.
    """
    f = getattr(server, name)
    return cast("Callable[..., Any]", getattr(f, "fn", f))


def lookup(category: str, slug: str, env: str) -> str | None:
    try:
        return fn("lookup_id")(category=category, slug=slug, env=env)["id"]
    except Exception:
        return None


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
            for field in (
                "directory_column",
                "measurement_unit",
                "observations",
            ):
                if r[field]:
                    entry[field] = r[field]
            rows.append(entry)
    return json.dumps(rows, ensure_ascii=False)


def table_columns(table_id: str, env: str) -> dict[str, str]:
    """Column name -> bare uuid, from the uncapped query."""
    return {
        c["name"]: c["id"].split(":")[-1]
        for c in server._fetch_table_columns(table_id, env)
    }


def delete(kind: str, record_id: str, env: str) -> None:
    query = f"mutation($id: UUID!) {{ Delete{kind}(id: $id) {{ errors }} }}"
    payload = server._gql(query, {"id": record_id}, env=env)[f"Delete{kind}"]
    if payload and payload.get("errors"):
        raise RuntimeError(f"Delete{kind} {record_id}: {payload['errors']}")


def prune(node: dict, env: str) -> None:
    """Delete every duplicate child record beyond the first of each kind."""
    seen: set[str] = set()
    for level in node["observation_levels"]:
        if level["entity_id"] in seen:
            delete("ObservationLevel", level["id"], env)
        else:
            seen.add(level["entity_id"])
    for extra in node["cloud_tables"][1:]:
        delete("CloudTable", extra["id"], env)
    for extra in node["coverages"][1:]:
        delete("Coverage", extra["id"], env)
    seen = set()
    for upd in node["updates"]:
        if upd["entity_id"] in seen:
            delete("Update", upd["id"], env)
        else:
            seen.add(upd["entity_id"])


def existing(node: dict) -> dict:
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


def main(env: str, status: str) -> None:
    get_dataset = fn("get_dataset")
    print(f"registering {GCP_DATASET} on {env}\n")

    # --- reference ids, resolved per backend --------------------------------
    org = (
        lookup("organization", ORGANIZATION["slug"], env)
        or fn("create_update_organization")(
            slug=ORGANIZATION["slug"],
            name_pt=ORGANIZATION["name"],
            name_en=ORGANIZATION["name"],
            name_es=ORGANIZATION["name"],
            description_pt=ORGANIZATION["description_pt"],
            description_en=ORGANIZATION["description_en"],
            description_es=ORGANIZATION["description_es"],
            website=ORGANIZATION["website"],
            area_id=lookup("area", "us", env),
            env=env,
        )["id"]
    )

    tag_ids = []
    for slug in TAGS:
        found = lookup("tag", slug, env)
        if found:
            tag_ids.append(found)
    for slug, (pt, en, es) in NEW_TAGS.items():
        found = lookup("tag", slug, env)
        if not found:
            found = fn("create_update_tag")(
                slug=slug, name_pt=pt, name_en=en, name_es=es, env=env
            )["id"]
        tag_ids.append(found)

    entities = {e: lookup("entity", e, env) for e in ENTITIES}
    missing = [e for e, v in entities.items() if not v]
    if missing:
        raise SystemExit(f"entities not found on {env}: {missing}")
    area_us = lookup("area", "us", env)
    under_review = lookup("status", "under_review", env)
    published = lookup("status", "published", env)
    account = fn("get_authenticated_account")(env=env)["id"]

    # --- dataset ------------------------------------------------------------
    # The status is passed in, not inferred. A dataset is created
    # `under_review`, which hides it from the production frontend until the
    # onboarding PR has merged and the prod tables actually exist; publishing is
    # a separate deliberate step (onboarding-workflow step 13).
    current = get_dataset(slug=DATASET_SLUG, env=env)
    dataset_status = published if status == "published" else under_review
    dataset_id = fn("create_update_dataset")(
        slug=DATASET_SLUG,
        name_pt=DATASET_NAME[0],
        name_en=DATASET_NAME[1],
        name_es=DATASET_NAME[2],
        description_pt=DATASET_DESCRIPTION[0],
        description_en=DATASET_DESCRIPTION[1],
        description_es=DATASET_DESCRIPTION[2],
        organization_ids=[org],
        theme_ids=[lookup("theme", "economics", env)],
        tag_ids=tag_ids,
        status_id=dataset_status,
        id=current["id"] if current["found"] else None,
        env=env,
    )["id"]
    print(
        f"dataset {DATASET_SLUG} ({dataset_id}) status={status}, {len(tag_ids)} tags"
    )

    # --- raw data sources ---------------------------------------------------
    have_sources = {
        s["url"]: s["id"]
        for s in fn("get_raw_data_sources")(dataset_slug=DATASET_SLUG, env=env)
    }
    source_ids = {}
    for key, spec in SOURCES.items():
        source_ids[key] = fn("create_update_raw_data_source")(
            dataset_id=dataset_id,
            name_pt=spec["name"][0],
            name_en=spec["name"][1],
            name_es=spec["name"][2],
            description_pt=spec["description"][0],
            description_en=spec["description"][1],
            description_es=spec["description"][2],
            url=spec["url"],
            license_id=lookup("license", "cc0", env),
            availability_id=lookup("availability", "online", env),
            language_ids=[lookup("language", "en", env)],
            has_structured_data=True,
            contains_api=False,
            is_free=True,
            requires_registration=False,
            id=have_sources.get(spec["url"]),
            env=env,
        )["id"]
    print(f"raw data sources: {len(source_ids)}")

    # --- tables -------------------------------------------------------------
    current = get_dataset(slug=DATASET_SLUG, env=env)
    table_ids = {}
    for table in TABLE_ORDER:
        pt, en, es = NAMES[table]
        dpt, den, des = DESCRIPTIONS[table]
        node = current["tables"].get(table)
        table_ids[table] = fn("create_update_table")(
            slug=table,
            name_pt=pt,
            name_en=en,
            name_es=es,
            description_pt=dpt,
            description_en=den,
            description_es=des,
            dataset_id=dataset_id,
            status_id=published,
            published_by_ids=[account],
            data_cleaned_by_ids=[account],
            id=node["id"] if node else None,
            env=env,
        )["id"]

    # drop anything an earlier run duplicated, then reuse the surviving ids
    current = get_dataset(slug=DATASET_SLUG, env=env)
    for node in current["tables"].values():
        prune(node, env)
    current = get_dataset(slug=DATASET_SLUG, env=env)

    today = f"{datetime.date.today().isoformat()}T00:00:00"
    for table in TABLE_ORDER:
        table_id = table_ids[table]
        result = fn("bulk_upsert_columns")(
            table_id=table_id,
            columns_json=columns_payload(table),
            env=env,
            batch_size=50,
        )
        cols = table_columns(table_id, env)
        have = existing(current["tables"][table])

        for entity, names in GRAIN[table].items():
            level = fn("create_update_observation_level")(
                table_id=table_id,
                entity_id=entities[entity],
                id=have["levels"].get(entities[entity]),
                env=env,
            )["id"]
            for name in names:
                if name not in cols:
                    continue
                # update_column's booleans default False, so is_partition has to
                # be re-passed or the flag is clobbered
                fn("update_column")(
                    column_id=cols[name],
                    column_name=name,
                    table_id=table_id,
                    observation_level_id=level,
                    is_partition=(PARTITION.get(table) == name),
                    env=env,
                )

        fn("create_update_cloud_table")(
            table_id=table_id,
            gcp_project_id=GCP_PROJECT[env],
            gcp_dataset_id=GCP_DATASET,
            gcp_table_id=table,
            id=have["cloud"],
            env=env,
        )
        coverage_id = fn("create_update_coverage")(
            table_id=table_id,
            area_id=area_us,
            is_closed=False,
            id=have["coverage"],
            env=env,
        )["id"]
        span = COVERAGE[table]
        if span:
            # Granularity has to match the table's own. Quarterly tables need
            # months: year-only would report 2009..2026 for data that really
            # spans 2009-09..2026-06. The annual CRA tables take years only.
            start_year, start_month, end_year, end_month = span
            fn("create_update_datetime_range")(
                coverage_id=coverage_id,
                start_year=start_year,
                start_month=start_month,
                end_year=end_year,
                end_month=end_month,
                interval=1,
                id=have["range"],
                env=env,
            )
        # table Update: when WE last refreshed it, a wall clock
        update_entity = entities[UPDATE_ENTITY[table]]
        fn("create_update_update")(
            entity_id=update_entity,
            frequency=1,
            lag=1,
            latest=today,
            table_id=table_id,
            id=have["updates"].get(update_entity),
            env=env,
        )
        print(f"  {table:<22} columns={result['source_rows']:>4}")

    # Raw sources are linked in a deferred second pass: the backend rejects
    # CreateUpdateTable once that table's coverage carries a datetime range.
    for table in TABLE_ORDER:
        pt, en, es = NAMES[table]
        dpt, den, des = DESCRIPTIONS[table]
        # the API does no partial updates, so every required field is re-passed
        fn("create_update_table")(
            slug=table,
            name_pt=pt,
            name_en=en,
            name_es=es,
            description_pt=dpt,
            description_en=den,
            description_es=des,
            dataset_id=dataset_id,
            status_id=published,
            published_by_ids=[account],
            data_cleaned_by_ids=[account],
            raw_data_source_ids=[source_ids[TABLE_SOURCE[table]]],
            id=table_ids[table],
            env=env,
        )

    # raw source Update: what the SOURCE published, i.e. its max coverage date,
    # never today's date -- that would claim the publisher released data today
    for key, source_id in source_ids.items():
        fn("create_update_update")(
            entity_id=entities["quarter" if key != "cra" else "year"],
            frequency=1,
            latest=SOURCE_LATEST[key],
            raw_data_source_id=source_id,
            env=env,
        )

    current = get_dataset(slug=DATASET_SLUG, env=env)
    print()
    for table, node in sorted(current["tables"].items()):
        ranges = sum(
            len(c.get("datetime_ranges", [])) for c in node["coverages"]
        )
        levels = ",".join(
            sorted(o["entity_slug"] for o in node["observation_levels"])
        )
        print(
            f"{table:<22} cols={len(server._fetch_table_columns(node['id'], env)):<4} "
            f"OLs=[{levels}] cloud={len(node['cloud_tables'])} "
            f"coverage={len(node['coverages'])} ranges={ranges} "
            f"updates={len(node['updates'])}"
        )


if __name__ == "__main__":
    main(
        sys.argv[1] if len(sys.argv) > 1 else "staging",
        sys.argv[2] if len(sys.argv) > 2 else "under_review",
    )
