"""Register the us_eia_seds metadata in a Data Basis backend.

    python register.py --env staging
    python register.py --env prod --gcp-project basedosdados

Idempotent by construction: every record is looked up through ``get_dataset``
first and its id passed back on the write, because ``create_update_*`` creates a
duplicate when called without an id. Re-running is therefore safe and is the
intended way to apply an edit.

Text comes from ``metadata_spec.py``; column payloads come from the architecture
CSV through ``gen_columns_json.py``, so the backend cannot describe a different
schema from the transform and the dbt model.

The dataset is registered ``under_review`` — publishing is a separate, later
action. SEDS is annual, so every table is fully free: there is no BD Pro split.
"""

import argparse
import json

import databasis_mcp.tools.metadata as bd_mcp_metadata
import databasis_mcp.tools.write as bd_mcp_write

from models.us_eia_seds.code import gen_columns_json
from models.us_eia_seds.code import metadata_spec as spec
from models.us_eia_seds.code.common import DATA_TABLES, OUTPUT

ALL_TABLES = [*DATA_TABLES, "dicionario"]


def coverage_bounds(table: str) -> tuple[int, int]:
    """(start_year, end_year) read from the cleaned parquet. SEDS is annual."""
    import pyarrow.dataset as ds

    data = ds.dataset(OUTPUT / table, format="parquet")
    lo, hi = 9999, 0
    for batch in data.to_batches(columns=["year"]):
        for year in batch.column(0).to_pylist():
            if year is None:
                continue
            lo = min(lo, int(year))
            hi = max(hi, int(year))
    return lo, hi


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env", default="staging", choices=["dev", "staging", "prod"]
    )
    parser.add_argument("--gcp-project", default="basedosdados-dev")
    args = parser.parse_args()
    env = args.env

    ids = bd_mcp_metadata.discover_ids(
        env=env, keys=["status", "license", "availability", "theme", "entity"]
    )
    org = bd_mcp_metadata.lookup_id(
        category="organization",
        slug=spec.DATASET["organization_slugs"][0],
        env=env,
    )
    area = bd_mcp_metadata.lookup_id(category="area", slug="us", env=env)
    account = bd_mcp_metadata.get_authenticated_account(env=env)
    account_id = str(account["id"])

    # Tag SLUGS differ between backends (staging Portuguese, prod English) but the
    # UUIDs are preserved, so resolve on the target env and fall back to staging.
    tag_ids = []
    for slug in spec.DATASET["tag_slugs"]:
        try:
            tag_ids.append(
                bd_mcp_metadata.lookup_id(category="tag", slug=slug, env=env)[
                    "id"
                ]
            )
        except Exception:
            tag_ids.append(
                bd_mcp_metadata.lookup_id(
                    category="tag", slug=slug, env="staging"
                )["id"]
            )

    existing = bd_mcp_metadata.get_dataset(slug=spec.DATASET_SLUG, env=env)
    dataset_id = existing["id"] if existing["found"] else None

    dataset = bd_mcp_write.create_update_dataset(
        slug=spec.DATASET_SLUG,
        name_pt=spec.DATASET["name_pt"],
        name_en=spec.DATASET["name_en"],
        name_es=spec.DATASET["name_es"],
        description_pt=spec.DATASET["description_pt"],
        description_en=spec.DATASET["description_en"],
        description_es=spec.DATASET["description_es"],
        organization_ids=[org["id"]],
        theme_ids=[ids["theme"][s] for s in spec.DATASET["theme_slugs"]],
        tag_ids=tag_ids,
        status_id=ids["status"]["under_review"],
        id=dataset_id,
        env=env,
    )
    dataset_id = dataset["id"]
    print(f"dataset {spec.DATASET_SLUG} -> {dataset_id}")

    known_sources = {
        s.get("name") or s.get("name_pt"): s["id"]
        for s in bd_mcp_write.get_raw_data_sources(
            dataset_slug=spec.DATASET_SLUG, env=env
        )
    }
    source_ids = {}
    for key, source in spec.RAW_SOURCES.items():
        result = bd_mcp_write.create_update_raw_data_source(
            dataset_id=dataset_id,
            name_pt=source["name_pt"],
            name_en=source["name_en"],
            name_es=source["name_es"],
            description_pt=source["description_pt"],
            description_en=source["description_en"],
            description_es=source["description_es"],
            url=source["url"],
            # US government publications are in the public domain and EIA
            # explicitly permits redistribution; cc0 is the house mapping.
            # https://www.eia.gov/about/copyrights_reuse.php
            license_id=ids["license"]["cc0"],
            availability_id=ids["availability"]["online"],
            has_structured_data=True,
            is_free=True,
            contains_api=False,
            requires_registration=False,
            id=known_sources.get(source["name_pt"]),
            env=env,
        )
        source_ids[key] = result["id"]
        print(f"raw source {key} -> {result['id']}")

    current = bd_mcp_metadata.get_dataset(slug=spec.DATASET_SLUG, env=env)[
        "tables"
    ]

    for table in ALL_TABLES:
        entry = spec.TABLES[table]
        prior = current.get(table, {})
        result = bd_mcp_write.create_update_table(
            slug=table,
            name_pt=entry["name_pt"],
            name_en=entry["name_en"],
            name_es=entry["name_es"],
            description_pt=entry["description_pt"],
            description_en=entry["description_en"],
            description_es=entry["description_es"],
            dataset_id=dataset_id,
            status_id=ids["status"]["published"],
            published_by_ids=[account_id],
            data_cleaned_by_ids=[account_id],
            auxiliary_files_url=(
                "https://storage.googleapis.com/basedosdados/auxiliary_files/"
                f"{spec.GCP_DATASET_ID}/{table}/auxiliary_files.zip"
            )
            if table != "dicionario"
            else "",
            id=prior.get("id"),
            env=env,
        )
        table_id = result["id"]
        print(f"\ntable {table} -> {table_id}")

        payload = (
            gen_columns_json.DICIONARIO_COLUMNS
            if table == "dicionario"
            else gen_columns_json.payload(table)
        )
        bd_mcp_write.bulk_upsert_columns(
            table_id=table_id,
            columns_json=json.dumps(payload, ensure_ascii=False),
            env=env,
        )
        print(f"  {len(payload)} columns")

        by_entity = {
            ol["entity_slug"]: ol["id"]
            for ol in prior.get("observation_levels", [])
        }
        level_ids = {}
        for entity_slug in entry["observation_levels"]:
            level = bd_mcp_write.create_update_observation_level(
                table_id=table_id,
                entity_id=ids["entity"][entity_slug],
                id=by_entity.get(entity_slug),
                env=env,
            )
            level_ids[entity_slug] = level["id"]
        if level_ids:
            print(f"  observation levels: {sorted(level_ids)}")

        # Link each identifying column to its level, or the site renders the
        # level's columns as "Não informado". update_column's booleans default to
        # False, so is_partition has to be re-passed on `year`.
        refreshed = bd_mcp_metadata.get_dataset(
            slug=spec.DATASET_SLUG, env=env
        )["tables"][table]
        column_ids = {c["name"]: c["id"] for c in refreshed["columns"]}
        for entity_slug, column_name in entry["level_columns"].items():
            bd_mcp_write.update_column(
                column_id=column_ids[column_name],
                column_name=column_name,
                table_id=table_id,
                observation_level_id=level_ids[entity_slug],
                is_partition=(column_name == "year"),
                env=env,
            )

        prior_cloud = prior.get("cloud_tables", [])
        bd_mcp_write.create_update_cloud_table(
            table_id=table_id,
            gcp_project_id=args.gcp_project,
            gcp_dataset_id=spec.GCP_DATASET_ID,
            gcp_table_id=table,
            id=prior_cloud[0]["id"] if prior_cloud else None,
            env=env,
        )

        prior_cov = prior.get("coverages", [])
        if table == "dicionario":
            # No date column, so no datetime range — but it still needs a
            # Coverage so the table shows an area on the site.
            bd_mcp_write.create_update_coverage(
                table_id=table_id,
                area_id=area["id"],
                id=prior_cov[0]["id"] if prior_cov else None,
                env=env,
            )
            continue

        start_year, end_year = coverage_bounds(table)
        print(f"  coverage {start_year} .. {end_year}")
        cov = bd_mcp_write.create_update_coverage(
            table_id=table_id,
            area_id=area["id"],
            is_closed=False,
            id=prior_cov[0]["id"] if prior_cov else None,
            env=env,
        )
        ranges = prior_cov[0]["datetime_ranges"] if prior_cov else []
        bd_mcp_write.create_update_datetime_range(
            coverage_id=cov["id"],
            start_year=start_year,
            end_year=end_year,
            interval=1,
            is_closed=False,
            id=ranges[0]["id"] if ranges else None,
            env=env,
        )

    # Deferred: link the raw source now that it exists.
    current = bd_mcp_metadata.get_dataset(slug=spec.DATASET_SLUG, env=env)[
        "tables"
    ]
    for table in ALL_TABLES:
        entry = spec.TABLES[table]
        bd_mcp_write.create_update_table(
            slug=table,
            name_pt=entry["name_pt"],
            name_en=entry["name_en"],
            name_es=entry["name_es"],
            description_pt=entry["description_pt"],
            description_en=entry["description_en"],
            description_es=entry["description_es"],
            dataset_id=dataset_id,
            status_id=ids["status"]["published"],
            published_by_ids=[account_id],
            data_cleaned_by_ids=[account_id],
            raw_data_source_ids=[source_ids[spec.TABLE_RAW_SOURCE[table]]],
            id=current[table]["id"],
            env=env,
        )
    print("\nraw source linked (exactly one per table)")

    bd_mcp_write.reorder_tables(
        dataset_slug=spec.DATASET_SLUG, table_slugs=ALL_TABLES, env=env
    )
    print(f"table order: {ALL_TABLES}")


if __name__ == "__main__":
    main()
