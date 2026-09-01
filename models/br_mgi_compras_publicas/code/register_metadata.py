"""Register the br_mgi_compras_publicas metadata in the Data Basis backend.

    ~/.pyenv/versions/3.11.6/bin/python \
        models/br_mgi_compras_publicas/code/register_metadata.py [staging|prod] [under_review|published]

Everything is resolved by slug at runtime, because reference ids differ between
backends, and the script is idempotent: a re-run updates rather than duplicating,
and a second run is a no-op.

It calls the databasis MCP server's functions in-process rather than through the
MCP tool layer. Same code path, but it makes a 534-column payload practical --
as tool arguments those would be hundreds of KB of JSON.

Three backend behaviours this works around, each of which has cost real time on
previous onboardings:

* **`create_update_*` is not idempotent for a table's child records.**
  Observation levels, cloud tables, coverages and updates get a brand-new row
  whenever `id` is omitted, so a re-run silently multiplies them. Existing ids
  are read back and passed, and `prune()` clears any duplicates already there.
* **Duplicate coverages then break `create_update_table`** with
  `'TableForm' has no field named 'coverages_areas'`, an error naming nothing
  relevant. It only shows up on tables whose coverage carries a datetime range.
* **`bulk_upsert_columns` does not link observation levels.** Each identifying
  column needs a separate `update_column` call, and because that call's boolean
  arguments default to False, `is_partition` must be re-passed in the same call
  or it is silently cleared.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(Path.home() / "Dropbox/BD/mcp"))

import server  # noqa: E402  # pyrefly: ignore [missing-import]  (resolved above)
from dbt_spec import TABLES as DBT  # noqa: E402
from observation_translations import (  # noqa: E402
    OBSERVATIONS,
    check_translations,
)
from table_metadata import DATASET, TABLE_ORDER  # noqa: E402
from table_metadata import TABLES as META  # noqa: E402

ARCH = HERE / "architecture"
DATASET_ID = "br_mgi_compras_publicas"
GCP_PROJECTS = {
    "staging": "basedosdados-dev",
    "dev": "basedosdados-dev",
    "prod": "basedosdados",
}
LICENSE_SLUG = "cc_40"  # CC BY 4.0, declared by the API itself
AVAILABILITY_SLUG = "online"
AREA_SLUG = "br"

RAW_SOURCE = {
    "name_pt": "API Compras.gov.br",
    "name_en": "Compras.gov.br API",
    "name_es": "API Compras.gov.br",
    "description_pt": (
        "API pública de dados abertos do Compras.gov.br, que expõe contratações, itens, "
        "resultados, atas de registro de preços, contratos e os cadastros de órgãos, unidades "
        "compradoras, fornecedores e catálogos."
    ),
    "description_en": (
        "Public open-data API of Compras.gov.br, exposing procurements, items, results, price "
        "records, contracts and the registers of bodies, purchasing units, suppliers and "
        "catalogues."
    ),
    "description_es": (
        "API pública de datos abiertos de Compras.gov.br, que expone contrataciones, ítems, "
        "resultados, actas de registro de precios, contratos y los registros de órganos, "
        "unidades compradoras, proveedores y catálogos."
    ),
    "url": "https://dadosabertos.compras.gov.br/swagger-ui/index.html",
}


def fn(name: str) -> Callable[..., Any]:
    """The plain function behind an MCP tool.

    FastMCP's decorator keeps the original callable on `.fn`; annotating the
    return type keeps call sites type-checkable, since `getattr` alone reads as
    `Any | None` to the checker.
    """
    f = getattr(server, name)
    return cast("Callable[..., Any]", getattr(f, "fn", f))


def lookup(category: str, slug: str, env: str) -> str | None:
    try:
        return fn("lookup_id")(category=category, slug=slug, env=env)["id"]
    except Exception:
        return None


def read_architecture(table: str) -> list[dict[str, str]]:
    import csv

    with (ARCH / f"{table}.csv").open(encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def columns_payload(table: str) -> str:
    """Every column of a table as bulk_upsert_columns' columns_json.

    Descriptions and observations are sent in all three languages. A caller that
    passes only the bare `observations` key leaves EN and ES blank, which is how
    3,022 production columns ended up Portuguese-only.
    """
    rows = []
    for column in read_architecture(table):
        entry: dict[str, Any] = {
            "name": column["name"],
            "bigquery_type": column["bigquery_type"],
            "description_pt": column["description"],
            "description_en": column["description_en"],
            "description_es": column["description_es"],
            "covered_by_dictionary": column["covered_by_dictionary"] == "yes",
            "has_sensitive_data": column["has_sensitive_data"] == "yes",
        }
        if column["directory_column"]:
            entry["directory_column"] = column["directory_column"]
        if column["measurement_unit"]:
            entry["measurement_unit"] = column["measurement_unit"]
        if column["temporal_coverage"]:
            entry["temporal_coverage"] = column["temporal_coverage"]
        note = column["observations"].strip()
        if note:
            en, es = OBSERVATIONS[note]
            entry["observations_pt"] = note
            entry["observations_en"] = en
            entry["observations_es"] = es
        rows.append(entry)
    return json.dumps(rows, ensure_ascii=False)


def existing(node: dict[str, Any]) -> dict[str, Any]:
    """Index a table node's child records so their ids can be reused."""
    return {
        "observation_levels": {
            level.get("entity_id"): level["id"]
            for level in node.get("observation_levels", [])
        },
        "cloud_tables": [c["id"] for c in node.get("cloud_tables", [])],
        "coverages": [c["id"] for c in node.get("coverages", [])],
        "updates": [u["id"] for u in node.get("updates", [])],
    }


def prune(node: dict[str, Any], env: str) -> None:
    """Delete duplicate child records left by earlier non-idempotent runs.

    Duplicate coverages are not merely untidy: they make a later
    create_update_table fail with an error that names `coverages_areas`, a field
    that appears nowhere in the request.
    """
    delete = fn("delete_record") if hasattr(server, "delete_record") else None
    if delete is None:
        return
    seen: set[Any] = set()
    for level in node.get("observation_levels", []):
        key = level.get("entity_id")
        if key in seen:
            delete(kind="observationlevel", record_id=level["id"], env=env)
        seen.add(key)
    for kind, records in (
        ("coverage", node.get("coverages", [])),
        ("update", node.get("updates", [])),
    ):
        for extra in records[1:]:
            delete(kind=kind, record_id=extra["id"], env=env)


def main(env: str, status: str) -> int:
    used = {
        c["observations"].strip()
        for t in TABLE_ORDER
        for c in read_architecture(t)
    }
    missing = check_translations(used)
    if missing:
        print("observations with no EN/ES rendering:")
        for note in missing:
            print("  -", note)
        return 1

    account = fn("get_authenticated_account")(env=env)
    account_id = account["id"]
    status_id = lookup("status", status, env)
    org_id = lookup("organization", "mgi", env)
    license_id = lookup("license", LICENSE_SLUG, env)
    availability_id = lookup("availability", AVAILABILITY_SLUG, env)
    area_id = lookup("area", AREA_SLUG, env)
    theme_ids = [lookup("theme", slug, env) for slug in DATASET["themes"]]
    tag_ids = [lookup("tag", slug, env) for slug in DATASET["tags"]]
    if not all([account_id, status_id, org_id, license_id, area_id]):
        print("could not resolve a required reference id")
        return 1
    missing_tags = [
        s for s, i in zip(DATASET["tags"], tag_ids, strict=True) if not i
    ]
    if missing_tags:
        print(f"tags not found in {env}: {missing_tags}")
        return 1

    node = fn("get_dataset")(slug=DATASET["slug"], env=env)
    dataset_id = fn("create_update_dataset")(
        id=node.get("id"),
        slug=DATASET["slug"],
        name_pt=DATASET["name_pt"],
        name_en=DATASET["name_en"],
        name_es=DATASET["name_es"],
        description_pt=DATASET["description_pt"],
        description_en=DATASET["description_en"],
        description_es=DATASET["description_es"],
        organization_ids=[org_id],
        theme_ids=[t for t in theme_ids if t],
        tag_ids=[t for t in tag_ids if t],
        status_id=status_id,
        env=env,
    )["id"]
    print(f"dataset {DATASET['slug']} -> {dataset_id} ({status})")

    sources = fn("get_raw_data_sources")(dataset_slug=DATASET["slug"], env=env)
    source_id = None
    for candidate in (
        sources
        if isinstance(sources, list)
        else sources.get("raw_data_sources", [])
    ):
        if candidate.get("url") == RAW_SOURCE["url"]:
            source_id = candidate["id"]
    source_id = fn("create_update_raw_data_source")(
        id=source_id,
        dataset_id=dataset_id,
        name_pt=RAW_SOURCE["name_pt"],
        name_en=RAW_SOURCE["name_en"],
        name_es=RAW_SOURCE["name_es"],
        description_pt=RAW_SOURCE["description_pt"],
        description_en=RAW_SOURCE["description_en"],
        description_es=RAW_SOURCE["description_es"],
        url=RAW_SOURCE["url"],
        availability_id=availability_id,
        license_id=license_id,
        # No area_ids: a raw data source carries no geographic coverage in this
        # backend, unlike a table. Passing it is a TypeError, not a no-op.
        contains_api=True,
        is_free=True,
        requires_registration=False,
        status_id=status_id,
        env=env,
    )["id"]
    print(f"raw data source -> {source_id}")

    published_status = lookup("status", "published", env)
    node = fn("get_dataset")(slug=DATASET["slug"], env=env)
    table_ids: dict[str, str] = {}

    for table in TABLE_ORDER:
        meta = META[table]
        spec = DBT[table]
        current = node.get("tables", {}).get(table, {})
        prune(current, env)
        prior = existing(current)

        table_id = fn("create_update_table")(
            id=current.get("id"),
            slug=table,
            name_pt=meta.name_pt,
            name_en=meta.name_en,
            name_es=meta.name_es,
            description_pt=spec.description,
            description_en=meta.description_en,
            description_es=meta.description_es,
            dataset_id=dataset_id,
            status_id=published_status,
            published_by_ids=[account_id],
            data_cleaned_by_ids=[account_id],
            env=env,
        )["id"]
        table_ids[table] = table_id

        level_ids: dict[str, str] = {}
        for entity_slug, column_name in meta.observation_levels.items():
            entity_id = lookup("entity", entity_slug, env)
            if not entity_id:
                print(f"  {table}: entity {entity_slug} not found")
                continue
            level_ids[column_name] = fn("create_update_observation_level")(
                id=prior["observation_levels"].get(entity_id),
                table_id=table_id,
                entity_id=entity_id,
                env=env,
            )["id"]

        result = fn("bulk_upsert_columns")(
            table_id=table_id, columns_json=columns_payload(table), env=env
        )
        # bulk_upsert_columns reports counts, not ids, and update_column needs
        # the id. _fetch_table_columns uses the uncapped allColumn query rather
        # than get_dataset's nested columns(first: 200), which silently truncates
        # on a wide table.
        # allColumn returns Relay global ids ("ColumnNode:<uuid>"), while
        # update_column wants the bare uuid -- passing the prefixed form fails
        # with "não é um UUID válido" naming no field.
        column_ids = {
            c["name"]: c["id"].split(":", 1)[-1]
            for c in fn("_fetch_table_columns")(table_id=table_id, env=env)
        }

        # bulk_upsert_columns does not link observation levels, and
        # update_column's booleans default to False -- so is_partition has to be
        # re-passed here or the bulk step's flag is silently cleared.
        for column_name, level_id in level_ids.items():
            fn("update_column")(
                column_id=column_ids[column_name],
                column_name=column_name,
                table_id=table_id,
                observation_level_id=level_id,
                is_partition=column_name == spec.partition,
                env=env,
            )
        if spec.partition and spec.partition not in level_ids:
            fn("update_column")(
                column_id=column_ids[spec.partition],
                column_name=spec.partition,
                table_id=table_id,
                is_partition=True,
                env=env,
            )

        fn("create_update_cloud_table")(
            id=prior["cloud_tables"][0] if prior["cloud_tables"] else None,
            table_id=table_id,
            gcp_project_id=GCP_PROJECTS[env],
            gcp_dataset_id=DATASET_ID,
            gcp_table_id=table,
            env=env,
        )

        if meta.coverage:
            coverage_id = fn("create_update_coverage")(
                id=prior["coverages"][0] if prior["coverages"] else None,
                table_id=table_id,
                area_id=area_id,
                env=env,
            )["id"]
            start_year, start_month, end_year, end_month = meta.coverage
            fn("create_update_datetime_range")(
                coverage_id=coverage_id,
                start_year=start_year,
                start_month=start_month,
                end_year=end_year,
                end_month=end_month,
                interval=1,
                env=env,
            )

        counts = result if isinstance(result, dict) else {}
        print(
            f"  {table:<28} id={table_id[:8]}… columns="
            f"{counts.get('created', '?')}+{counts.get('updated', '?')} "
            f"levels={len(level_ids)}"
        )

    # reorder_tables keys on the dataset SLUG, not its id.
    fn("reorder_tables")(
        dataset_slug=DATASET["slug"], table_slugs=TABLE_ORDER, env=env
    )
    print(f"\nregistered {len(table_ids)} tables in {env}")
    return 0


if __name__ == "__main__":
    environment = sys.argv[1] if len(sys.argv) > 1 else "staging"
    dataset_status = sys.argv[2] if len(sys.argv) > 2 else "under_review"
    raise SystemExit(main(environment, dataset_status))
