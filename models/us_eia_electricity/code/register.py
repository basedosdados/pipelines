"""Register the us_eia_electricity metadata in a Data Basis backend.

    python register.py --env staging
    python register.py --env prod --gcp-project basedosdados

Idempotent by construction: every record is looked up through ``get_dataset``
first and its id passed back on the write, because ``create_update_*`` **creates
a duplicate** when called without an id. Re-running is therefore safe and is the
intended way to apply an edit.

Text comes from ``metadata_spec.py``; column payloads come from the architecture
CSVs through ``gen_columns_json.py``, so the backend cannot describe a different
schema from the transform and the dbt models.

Two things this deliberately does **not** do:

* it never sets ``status.published`` — the dataset is registered
  ``under_review`` and published as a separate, later action;
* it never writes coverage ``is_closed`` implicitly. The free/pro split is
  written explicitly by ``--bdpro``, because it is what makes the paywall real.
"""

import argparse
import json
import sys
from pathlib import Path

import metadata_spec as spec
from common import DATA_TABLES, OUTPUT, load_cols

sys.path.insert(
    0, "/Users/rdahis/Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp"
)
import server

CODE_DIR = Path(__file__).resolve().parent
ALL_TABLES = [*DATA_TABLES, "dicionario"]

# EIA-923's two tables refresh monthly, so they carry the BD Pro rolling window;
# EIA-860's two are annual and stay fully free. The pipeline recomputes the
# window on every run — what is registered here only has to be right until the
# first armed run.
BDPRO_TABLES = ["generation_fuel", "fuel_receipts_costs"]
FREE_LAG_MONTHS = 6


def coverage_bounds(table: str) -> tuple[int, int | None, int, int | None]:
    """(start_year, start_month, end_year, end_month) read from the parquet."""
    import pyarrow.dataset as ds

    columns = [c.name for c in load_cols(table)]
    monthly = "month" in columns
    data = ds.dataset(OUTPUT / table, format="parquet")
    lo = (9999, 99)
    hi = (0, 0)
    for batch in data.to_batches(
        columns=["year", "month"] if monthly else ["year"]
    ):
        years = batch.column(0).to_pylist()
        months = (
            batch.column(1).to_pylist() if monthly else [None] * len(years)
        )
        for year, month in zip(years, months, strict=True):
            if year is None:
                continue
            key = (int(year), int(month) if month else 1)
            lo = min(lo, key)
            hi = max(hi, key)
    if not monthly:
        return lo[0], None, hi[0], None
    return lo[0], lo[1], hi[0], hi[1]


def shift_months(year: int, month: int, delta: int) -> tuple[int, int]:
    index = year * 12 + (month - 1) + delta
    return index // 12, index % 12 + 1


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env", default="staging", choices=["dev", "staging", "prod"]
    )
    parser.add_argument("--gcp-project", default="basedosdados-dev")
    parser.add_argument(
        "--bdpro",
        action="store_true",
        help="also write the free/pro coverage split",
    )
    parser.add_argument(
        "--columns-dir", type=Path, default=CODE_DIR / "_columns"
    )
    args = parser.parse_args()
    env = args.env

    ids = server.discover_ids(
        env=env, keys=["status", "license", "availability", "theme", "entity"]
    )
    org = server.lookup_id(
        category="organization",
        slug=spec.DATASET["organization_slugs"][0],
        env=env,
    )
    area = server.lookup_id(category="area", slug="us", env=env)
    account = server.get_authenticated_account(env=env)
    account_id = str(account["id"])

    tag_ids = []
    for slug in spec.DATASET["tag_slugs"]:
        tag_ids.append(
            server.lookup_id(category="tag", slug=slug, env=env)["id"]
        )

    existing = server.get_dataset(slug=spec.DATASET_SLUG, env=env)
    dataset_id = existing["id"] if existing["found"] else None

    dataset = server.create_update_dataset(
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
        # under_review until the PR merges, table-approve materialises prod and
        # the live tables are verified. Publishing is a separate later action.
        status_id=ids["status"]["under_review"],
        id=dataset_id,
        env=env,
    )
    dataset_id = dataset["id"]
    print(f"dataset {spec.DATASET_SLUG} -> {dataset_id}")

    known_sources = {
        s.get("name") or s.get("name_pt"): s["id"]
        for s in server.get_raw_data_sources(
            dataset_slug=spec.DATASET_SLUG, env=env
        )
    }
    source_ids = {}
    for key, source in spec.RAW_SOURCES.items():
        result = server.create_update_raw_data_source(
            dataset_id=dataset_id,
            name_pt=source["name_pt"],
            name_en=source["name_en"],
            name_es=source["name_es"],
            description_pt=source["description_pt"],
            description_en=source["description_en"],
            description_es=source["description_es"],
            url=source["url"],
            # US government publications are in the public domain and EIA
            # explicitly permits redistribution; cc0 is the house mapping for a
            # US federal source. https://www.eia.gov/about/copyrights_reuse.php
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

    current = server.get_dataset(slug=spec.DATASET_SLUG, env=env)["tables"]

    for table in ALL_TABLES:
        entry = spec.TABLES[table]
        prior = current.get(table, {})
        result = server.create_update_table(
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

        payload = json.loads(
            (args.columns_dir / f"columns_{table}.json").read_text()
        )
        server.bulk_upsert_columns(
            table_id=table_id,
            columns_json=json.dumps(payload, ensure_ascii=False),
            env=env,
        )
        print(f"  {len(payload)} columns")

        # Observation levels. create_update_observation_level duplicates when
        # called without an id, so reuse the id already on the table.
        by_entity = {
            ol["entity_slug"]: ol["id"]
            for ol in prior.get("observation_levels", [])
        }
        level_ids = {}
        for entity_slug in entry["observation_levels"]:
            level = server.create_update_observation_level(
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
        refreshed = server.get_dataset(slug=spec.DATASET_SLUG, env=env)[
            "tables"
        ][table]
        column_ids = {c["name"]: c["id"] for c in refreshed["columns"]}
        for entity_slug, column_name in entry["level_columns"].items():
            server.update_column(
                column_id=column_ids[column_name],
                column_name=column_name,
                table_id=table_id,
                observation_level_id=level_ids[entity_slug],
                is_partition=(column_name == "year"),
                env=env,
            )

        prior_cloud = prior.get("cloud_tables", [])
        server.create_update_cloud_table(
            table_id=table_id,
            gcp_project_id=args.gcp_project,
            gcp_dataset_id=spec.GCP_DATASET_ID,
            gcp_table_id=table,
            id=prior_cloud[0]["id"] if prior_cloud else None,
            env=env,
        )

        if table == "dicionario":
            # No date column, so no coverage range. It still needs a Coverage so
            # the table shows an area on the site.
            prior_cov = prior.get("coverages", [])
            server.create_update_coverage(
                table_id=table_id,
                area_id=area["id"],
                id=prior_cov[0]["id"] if prior_cov else None,
                env=env,
            )
            continue

        start_year, start_month, end_year, end_month = coverage_bounds(table)
        print(
            f"  coverage {start_year}-{start_month} .. {end_year}-{end_month}"
        )

        wants_pro = args.bdpro and table in BDPRO_TABLES
        prior_cov = {c["id"]: c for c in prior.get("coverages", [])}
        prior_list = list(prior_cov.values())

        if not wants_pro:
            cov = server.create_update_coverage(
                table_id=table_id,
                area_id=area["id"],
                is_closed=False,
                id=prior_list[0]["id"] if prior_list else None,
                env=env,
            )
            ranges = prior_list[0]["datetime_ranges"] if prior_list else []
            server.create_update_datetime_range(
                coverage_id=cov["id"],
                start_year=start_year,
                start_month=start_month,
                end_year=end_year,
                end_month=end_month,
                interval=1,
                is_closed=False,
                id=ranges[0]["id"] if ranges else None,
                env=env,
            )
            continue

        # part_bdpro: free ends free_end INCLUSIVE, pro starts the next month, so
        # the two ranges are mutually exclusive. This mirrors exactly what
        # register_table_materialization_task recomputes on every pipeline run.
        free_end_year, free_end_month = shift_months(
            end_year, end_month, -FREE_LAG_MONTHS
        )
        pro_start_year, pro_start_month = shift_months(
            free_end_year, free_end_month, 1
        )
        print(
            f"  BD Pro: free {start_year}-{start_month:02d}..{free_end_year}-{free_end_month:02d}, "
            f"pro {pro_start_year}-{pro_start_month:02d}..{end_year}-{end_month:02d}"
        )
        free_prior = next(
            (c for c in prior_list if not c.get("is_closed")), None
        )
        pro_prior = next((c for c in prior_list if c.get("is_closed")), None)

        free = server.create_update_coverage(
            table_id=table_id,
            area_id=area["id"],
            is_closed=False,
            id=free_prior["id"] if free_prior else None,
            env=env,
        )
        server.create_update_datetime_range(
            coverage_id=free["id"],
            start_year=start_year,
            start_month=start_month,
            end_year=free_end_year,
            end_month=free_end_month,
            interval=1,
            is_closed=False,
            id=(free_prior or {}).get("datetime_ranges", [{}])[0].get("id")
            if free_prior
            else None,
            env=env,
        )
        pro = server.create_update_coverage(
            table_id=table_id,
            area_id=area["id"],
            is_closed=True,
            id=pro_prior["id"] if pro_prior else None,
            env=env,
        )
        server.create_update_datetime_range(
            coverage_id=pro["id"],
            start_year=pro_start_year,
            start_month=pro_start_month,
            end_year=end_year,
            end_month=end_month,
            interval=1,
            is_closed=True,
            id=(pro_prior or {}).get("datetime_ranges", [{}])[0].get("id")
            if pro_prior
            else None,
            env=env,
        )

    # Deferred: link the raw source now that every source exists.
    current = server.get_dataset(slug=spec.DATASET_SLUG, env=env)["tables"]
    for table in ALL_TABLES:
        entry = spec.TABLES[table]
        server.create_update_table(
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
    print("\nraw sources linked (exactly one per table)")

    server.reorder_tables(
        dataset_slug=spec.DATASET_SLUG, table_slugs=ALL_TABLES, env=env
    )
    print(f"table order: {ALL_TABLES}")


if __name__ == "__main__":
    main()
