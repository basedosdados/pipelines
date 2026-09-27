"""Register the us_eia_consumption metadata in a Data Basis backend.

    python register.py --env staging
    python register.py --env prod --gcp-project basedosdados --bdpro

Idempotent: every record is looked up through ``get_dataset`` first and its id
passed back on the write. The dataset is registered ``under_review``; publishing
is a separate later action.

eia861m refreshes monthly, so it carries the BD Pro rolling window (``--bdpro``
writes the free/pro coverage split); the three annual tables stay fully free.

This dataset previously (on the abandoned combined branch) also held a
``seds_consumption`` table; SEDS now lives in us_eia_seds, so any stale
``seds_consumption`` table is removed here.
"""

import argparse
import json
import os
import sys
from pathlib import Path

import gen_columns_json
import metadata_spec as spec
from common import DATA_TABLES, OUTPUT, load_cols

_MCP_PATH = os.environ.get(
    "BD_MCP_PATH",
    str(Path.home() / "Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp"),
)
if Path(_MCP_PATH).is_dir():
    sys.path.insert(0, _MCP_PATH)
try:
    import server
except ModuleNotFoundError as error:  # pragma: no cover
    raise SystemExit(
        "the Data Basis MCP `server` module is not importable. Point BD_MCP_PATH "
        f"at a checkout of the mcp repository (tried {_MCP_PATH!r})."
    ) from error

ALL_TABLES = [*DATA_TABLES, "dicionario"]
BDPRO_TABLES = ["eia861m"]
FREE_LAG_MONTHS = 6


def _first_range_id(coverage: dict | None) -> str | None:
    ranges = (coverage or {}).get("datetime_ranges") or []
    return ranges[0]["id"] if ranges else None


def coverage_bounds(table: str) -> tuple[int, int | None, int, int | None]:
    """(start_year, start_month, end_year, end_month) from the parquet."""
    import pyarrow.dataset as ds

    columns = [c.name for c in load_cols(table)]
    monthly = "month" in columns
    data = ds.dataset(OUTPUT / table, format="parquet")
    lo, hi = (9999, 99), (0, 0)
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
            lo, hi = min(lo, key), max(hi, key)
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
        "--bdpro", action="store_true", help="write the free/pro split"
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
        try:
            tag_ids.append(
                server.lookup_id(category="tag", slug=slug, env=env)["id"]
            )
        except Exception:
            tag_ids.append(
                server.lookup_id(category="tag", slug=slug, env="staging")[
                    "id"
                ]
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
        status_id=ids["status"]["under_review"],
        id=dataset_id,
        env=env,
    )
    dataset_id = dataset["id"]
    print(f"dataset {spec.DATASET_SLUG} -> {dataset_id}")

    # Remove the stale seds_consumption table left by the abandoned combined
    # branch — SEDS is its own dataset now.
    current_tables = server.get_dataset(slug=spec.DATASET_SLUG, env=env)[
        "tables"
    ]
    if "seds_consumption" in current_tables and hasattr(
        server, "delete_table"
    ):
        try:
            server.delete_table(
                table_id=current_tables["seds_consumption"]["id"], env=env
            )
            print("removed stale seds_consumption table")
        except Exception as exc:
            print(
                f"could not remove seds_consumption ({exc}); remove it manually"
            )

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

        payload = (
            gen_columns_json.DICIONARIO_COLUMNS
            if table == "dicionario"
            else gen_columns_json.payload(table)
        )
        server.bulk_upsert_columns(
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
            level = server.create_update_observation_level(
                table_id=table_id,
                entity_id=ids["entity"][entity_slug],
                id=by_entity.get(entity_slug),
                env=env,
            )
            level_ids[entity_slug] = level["id"]
        if level_ids:
            print(f"  observation levels: {sorted(level_ids)}")

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

        prior_cov = prior.get("coverages", [])
        if table == "dicionario":
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
        prior_list = list({c["id"]: c for c in prior_cov}.values())

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
            id=_first_range_id(free_prior),
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
            id=_first_range_id(pro_prior),
            env=env,
        )

    # Deferred raw-source link now that every source exists.
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
