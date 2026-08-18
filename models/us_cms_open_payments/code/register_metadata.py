"""Register the whole dataset in the Data Basis backend.

The databasis MCP server is a plain module, so its tool functions can be
driven from a script. That matters here: 23 tables and 651 columns is roughly
250 backend operations, and doing them one interactive call at a time invites
transcription errors. Everything is keyed by name or slug and every write is
an upsert, so the script is safe to re-run.

    ~/.pyenv/versions/3.11.6/bin/python register_metadata.py staging
    ~/.pyenv/versions/3.11.6/bin/python register_metadata.py prod --publish

The dataset is created `under_review` in every environment. `--publish` flips
it to published, which is only correct on dev/staging before promotion, or on
prod after the PR has merged and the tables are verified.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path.home() / "Dropbox" / "BD" / "mcp"))
import server

sys.path.insert(0, str(Path(__file__).resolve().parent))
import constants as c
import dataset_meta as meta
import layout
from table_descriptions import TABLE_DESCRIPTIONS

AREA_US = "61a2c232-c649-4b41-a5a3-1467b7393e11"
STATUS = {
    "under_review": "47208305-325a-4da9-9222-ac6849405b78",
    "published": "e16221de-ac30-4926-83d3-de219998dab3",
}
GCP_PROJECT = {
    "staging": "basedosdados-dev",
    "dev": "basedosdados-dev",
    "prod": "basedosdados",
}

# When the tables were last refreshed at Data Basis. The table-anchored Update
# is a wall clock, not a coverage date.
REFRESHED_ON = "2026-08-18"
ENTITY_YEAR = meta.ENTITY_IDS["year"]


def log(message: str) -> None:
    print(message, flush=True)


def register_dataset(env: str, publish: bool) -> str:
    existing = server.get_dataset(slug=meta.DATASET["slug"], env=env)
    status = STATUS["published" if publish else "under_review"]
    result = server.create_update_dataset(
        slug=meta.DATASET["slug"],
        name_pt=meta.DATASET["name_pt"],
        name_en=meta.DATASET["name_en"],
        name_es=meta.DATASET["name_es"],
        description_pt=meta.DATASET["description_pt"],
        description_en=meta.DATASET["description_en"],
        description_es=meta.DATASET["description_es"],
        organization_ids=[meta.ORGANIZATION_ID],
        theme_ids=meta.THEME_IDS,
        tag_ids=meta.TAG_IDS,
        status_id=status,
        id=existing.get("id"),
        env=env,
    )
    log(f"dataset {result['slug']} -> {result['id']}")
    return result["id"]


def register_raw_sources(dataset_id: str, env: str) -> dict[str, str]:
    existing = {
        s["name"]: s["id"]
        for s in server.get_raw_data_sources(
            dataset_slug=meta.DATASET["slug"], env=env
        ).get("raw_data_sources", [])
    }
    ids = {}
    for key, spec in meta.RAW_SOURCES.items():
        result = server.create_update_raw_data_source(
            dataset_id=dataset_id,
            name_pt=spec["name_pt"],
            name_en=spec["name_en"],
            name_es=spec["name_es"],
            description_pt=spec["description_pt"],
            description_en=spec["description_en"],
            description_es=spec["description_es"],
            url=spec["url"],
            license_id=meta.LICENSE_ID,
            availability_id=meta.AVAILABILITY_ID,
            has_structured_data=True,
            is_free=True,
            contains_api=True,
            requires_registration=False,
            id=existing.get(spec["name_pt"]),
            env=env,
        )
        ids[key] = result["id"]
        log(f"  raw source {key} -> {result['id']}")
    return ids


def raw_source_for(table: str) -> str:
    for key, spec in meta.RAW_SOURCES.items():
        if table in spec["tables"]:
            return key
    raise KeyError(table)


def table_coverage(table: str) -> tuple[int, int]:
    if table in layout.HEADERS["profile"]:
        return meta.ENTITY_TABLE_COVERAGE
    years = layout.COVERAGE.get(table)
    if years:
        return years[0], years[-1]
    return c.ALL_YEARS[0], c.ALL_YEARS[-1]


def register_table(
    table: str, dataset_id: str, account_id: str, env: str, snapshot: dict
) -> str:
    name_pt, name_en, name_es = meta.TABLE_NAMES[table]
    desc_pt, desc_en, desc_es = TABLE_DESCRIPTIONS[table]
    existing = snapshot.get("tables", {}).get(table, {})

    result = server.create_update_table(
        slug=table,
        name_pt=name_pt,
        name_en=name_en,
        name_es=name_es,
        description_pt=desc_pt,
        description_en=desc_en,
        description_es=desc_es,
        dataset_id=dataset_id,
        status_id=STATUS["published"],
        published_by_ids=[account_id],
        data_cleaned_by_ids=[account_id],
        id=existing.get("id"),
        env=env,
    )
    table_id = result["id"]
    log(f"\n{table} -> {table_id}")

    observation_levels = {}
    for entity in meta.OBSERVATION_LEVELS[table]:
        level = server.create_update_observation_level(
            table_id=table_id, entity_id=meta.ENTITY_IDS[entity], env=env
        )
        observation_levels[entity] = level["id"]
    if observation_levels:
        log(f"  observation levels: {', '.join(observation_levels)}")

    payload = (c.ARCH_DIR / "payloads" / f"{table}.json").read_text()
    upserted = server.bulk_upsert_columns(
        table_id=table_id, columns_json=payload, env=env
    )
    log(f"  columns: {json.dumps(upserted)[:160]}")

    column_ids = {
        col["name"]: col["id"]
        for col in server.get_dataset(slug=meta.DATASET["slug"], env=env)
        .get("tables", {})
        .get(table, {})
        .get("columns", [])
    }

    # Link each observation level to the column that identifies it, or the
    # site renders the level as "Não informado". update_column's booleans
    # default to False, so is_partition is re-passed in the same call.
    for entity, level_id in observation_levels.items():
        column = (
            "year"
            if entity == "year"
            else meta.OBSERVATION_LEVEL_COLUMN.get((table, entity))
        )
        if not column or column not in column_ids:
            continue
        server.update_column(
            column_id=column_ids[column],
            column_name=column,
            table_id=table_id,
            observation_level_id=level_id,
            is_partition=(
                column == "year" and table not in layout.UNPARTITIONED
            ),
            env=env,
        )

    server.create_update_cloud_table(
        table_id=table_id,
        gcp_project_id=GCP_PROJECT[env],
        gcp_dataset_id=c.GCP_DATASET_ID,
        gcp_table_id=table,
        id=(existing.get("cloud_tables") or [{}])[0].get("id"),
        env=env,
    )

    coverage = server.create_update_coverage(
        table_id=table_id,
        area_id=AREA_US,
        id=(existing.get("coverages") or [{}])[0].get("id"),
        env=env,
    )
    start, end = table_coverage(table)
    if table != "dicionario":
        ranges = (existing.get("coverages") or [{}])[0].get(
            "datetime_ranges"
        ) or [{}]
        server.create_update_datetime_range(
            coverage_id=coverage["id"],
            start_year=start,
            end_year=end,
            interval=1,
            id=ranges[0].get("id"),
            env=env,
        )
        server.create_update_update(
            entity_id=ENTITY_YEAR,
            frequency=1,
            latest=REFRESHED_ON,
            table_id=table_id,
            id=(existing.get("updates") or [{}])[0].get("id"),
            env=env,
        )
    log(f"  cloud table, coverage {start}-{end}, update recorded")
    return table_id


def main() -> None:
    env = sys.argv[1] if len(sys.argv) > 1 else "staging"
    publish = "--publish" in sys.argv

    account = server.get_authenticated_account(env=env)
    log(f"authenticated as {account['email']} (id {account['id']}) on {env}")

    dataset_id = register_dataset(env, publish)
    raw_sources = register_raw_sources(dataset_id, env)

    snapshot = server.get_dataset(slug=meta.DATASET["slug"], env=env)
    table_ids = {}
    for table in layout.LAYOUT:
        table_ids[table] = register_table(
            table, dataset_id, account["id"], env, snapshot
        )

    log("\nlinking raw data sources")
    for table, table_id in table_ids.items():
        name_pt, name_en, name_es = meta.TABLE_NAMES[table]
        desc_pt, desc_en, desc_es = TABLE_DESCRIPTIONS[table]
        server.create_update_table(
            slug=table,
            name_pt=name_pt,
            name_en=name_en,
            name_es=name_es,
            description_pt=desc_pt,
            description_en=desc_en,
            description_es=desc_es,
            dataset_id=dataset_id,
            status_id=STATUS["published"],
            published_by_ids=[account["id"]],
            data_cleaned_by_ids=[account["id"]],
            raw_data_source_ids=[raw_sources[raw_source_for(table)]],
            id=table_id,
            env=env,
        )
    log(f"  linked {len(table_ids)} tables to {len(raw_sources)} raw sources")

    server.reorder_tables(
        dataset_slug=meta.DATASET["slug"],
        table_slugs=list(layout.LAYOUT),
        env=env,
    )
    for table, table_id in table_ids.items():
        server.reorder_columns(
            table_id=table_id, column_names=layout.LAYOUT[table], env=env
        )
    log("\ntable and column order applied")


if __name__ == "__main__":
    main()
