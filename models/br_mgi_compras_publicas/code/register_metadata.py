"""Register the br_mgi_compras_publicas metadata in the Data Basis backend.

    uv run \
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

import datetime as dt
import json
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import databasis_mcp.tools.metadata as bd_mcp_metadata
import databasis_mcp.tools.write as bd_mcp_write

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[2]

from models.br_mgi_compras_publicas.code.dbt_spec import (  # noqa: E402
    TABLES as DBT,
)
from models.br_mgi_compras_publicas.code.observation_translations import (  # noqa: E402
    OBSERVATIONS,
    check_translations,
)
from models.br_mgi_compras_publicas.code.table_metadata import (  # noqa: E402
    DATASET,
    TABLE_ORDER,
    UPDATE_CADENCE,
)
from models.br_mgi_compras_publicas.code.table_metadata import (  # noqa: E402
    TABLES as META,
)

# The paywall tier per table, read from the pipeline's own declaration rather
# than restated here: `COVERAGE` is what the flow passes to
# `register_table_materialization_task`, so deriving the tier from it means the
# static registration and the flow cannot disagree about which tables are paid.
from pipelines.datasets.br_mgi_compras_publicas.constants import (  # noqa: E402
    COVERAGE,
)
from pipelines.utils.metadata.domain import PartBdpro  # noqa: E402

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

#: The dataset has two sources, and they are not interchangeable: the two
#: ComprasNet tables are scraped from HTML pages whose data, as their own
#: descriptions say, exists in no Compras.gov.br API. Linking the API source to
#: them states something false on the site.
COMPRASNET_SOURCE = {
    "name_pt": "ComprasNet — Consulta de Atas de Pregão (legado)",
    "name_en": "ComprasNet — Legacy reverse auction minutes search",
    "name_es": "ComprasNet — Consulta de actas de pregón (legado)",
    "description_pt": (
        "Páginas HTML do ComprasNet que detalham cada pregão eletrônico realizado sob a Lei "
        "8.666, consultáveis por janela de data da sessão. Fornecem o resultado por fornecedor, "
        "com marca, fabricante, modelo e a descrição do objeto ofertado, e o termo de "
        "homologação, com a linha do tempo de eventos de cada item. A ata da sessão lance a "
        "lance não é coletada: está protegida por CAPTCHA cujo texto declara que existe para "
        "impedir consulta automatizada. A fonte deixou de receber pregões na transição para a "
        "Lei 14.133, em janeiro de 2024."
    ),
    "description_en": (
        "ComprasNet HTML pages detailing each electronic reverse auction run under Law 8.666, "
        "searchable by session date window. They provide the result by supplier, with brand, "
        "manufacturer, model and the description of the object offered, and the award decision, "
        "with each item's event timeline. The bid-by-bid session record is not collected: it "
        "sits behind a CAPTCHA whose own text states it exists to prevent automated "
        "consultation. The source stopped receiving reverse auctions at the Law 14.133 "
        "transition, in January 2024."
    ),
    "description_es": (
        "Páginas HTML de ComprasNet que detallan cada pregón electrónico realizado bajo la Ley "
        "8.666, consultables por ventana de fecha de la sesión. Aportan el resultado por "
        "proveedor, con marca, fabricante, modelo y la descripción del objeto ofrecido, y el "
        "acta de homologación, con la línea de tiempo de eventos de cada ítem. El acta de la "
        "sesión puja por puja no se recolecta: está protegida por un CAPTCHA cuyo texto declara "
        "que existe para impedir la consulta automatizada. La fuente dejó de recibir pregones "
        "en la transición a la Ley 14.133, en enero de 2024."
    ),
    "url": "https://comprasnet.gov.br/livre/Pregao/ata0.asp",
    "contains_api": False,
}

#: Tables fed by COMPRASNET_SOURCE. Everything else comes from the API.
COMPRASNET_TABLES = frozenset({"pregao_item_oferta", "pregao_item_evento"})

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
    f = getattr(bd_mcp_metadata, name, None) or getattr(bd_mcp_write, name)
    return cast("Callable[..., Any]", getattr(f, "fn", f))


def lookup(category: str, slug: str, env: str) -> str | None:
    """Resolve a reference id by slug, since ids differ between backends.

    Args:
        category: discover_ids category, e.g. "status" or "tag".
        slug: the slug to resolve within that category.
        env: backend environment, "staging" or "prod".

    Returns:
        The id, or None when the slug does not exist in that backend.
    """
    try:
        return fn("lookup_id")(category=category, slug=slug, env=env)["id"]
    except Exception:
        return None


#: One timestamp for the whole run, so every table's Update reports the same
#: refresh rather than drifting by the seconds the run takes.
REFRESHED_AT = dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat()


#: The table-anchored Update's `latest` says when *we* last refreshed the table.
#: This script registers metadata; it materializes nothing. So a rerun that only
#: fixes a description must not move `latest`, or it reports a refresh that never
#: happened -- and `poll_source_for_update` compares the source's max coverage
#: date against this field, so an inflated value can leave a pipeline running
#: green while it ingests nothing.
#:
#: get_dataset does not return `latest`, so read it directly. On first creation
#: there is nothing to preserve and REFRESHED_AT stands; from then on the stored
#: value is carried forward and only a real materialization moves it, via the
#: pipeline's register_table_materialization_task.
_STORED_LATEST_QUERY = """
query($slug: String!) {
  allDataset(slug: $slug) {
    edges { node { tables { edges { node {
      slug updates { edges { node { latest } } }
    } } } } }
  }
}
"""


def stored_update_latest(slug: str, env: str) -> dict[str, str]:
    """Each table's current Update.latest, keyed by table slug."""
    try:
        data = bd_mcp_metadata._gql(
            _STORED_LATEST_QUERY, {"slug": slug}, env=env
        )
    except Exception as exc:
        print(f"  warning: could not read stored Update.latest ({exc})")
        return {}
    out: dict[str, str] = {}
    for edge in data.get("allDataset", {}).get("edges", []):
        for entry in edge["node"]["tables"]["edges"]:
            table = entry["node"]
            latest = [u["node"]["latest"] for u in table["updates"]["edges"]]
            if latest and latest[0]:
                out[table["slug"]] = latest[0]
    return out


#: Coverages and their ranges, with the free/pro discriminator.
#:
#: `get_dataset` returns a table's coverages as a bare list carrying no
#: `is_closed`, so the free and pro tiers are indistinguishable there -- and the
#: order is not stable: on prod `ata_registro_preco_item` lists the PRO coverage
#: first. Anything that reuses `coverages[0]` therefore overwrites the BD Pro
#: window on that table and the free window on the other six. Read the tier.
_COVERAGE_TIERS_QUERY = """
query($slug: String!) {
  allDataset(slug: $slug) {
    edges { node { tables { edges { node {
      slug
      coverages { edges { node {
        id isClosed
        datetimeRanges { edges { node { id } } }
      } } }
    } } } } }
  }
}
"""


def stored_coverages(
    slug: str, env: str
) -> dict[str, dict[bool, dict[str, Any]]]:
    """Each table's Coverages indexed by tier, as {table: {is_closed: {...}}}.

    Each entry carries the coverage `id` and its `range_ids`. A tier with more
    than one Coverage keeps the first and reports the rest, which is what
    `prune` deletes -- within the tier, never across it.
    """
    try:
        data = bd_mcp_metadata._gql(
            _COVERAGE_TIERS_QUERY, {"slug": slug}, env=env
        )
    except Exception as exc:
        # Fail loudly rather than falling back to positional reuse: guessing
        # here is what flattens the free/pro pair.
        sys.exit(f"could not read coverage tiers from {env}: {exc}")
    out: dict[str, dict[bool, dict[str, Any]]] = {}
    for edge in data.get("allDataset", {}).get("edges", []):
        for entry in edge["node"]["tables"]["edges"]:
            table = entry["node"]
            tiers: dict[bool, dict[str, Any]] = {}
            for wrapper in table["coverages"]["edges"]:
                coverage = wrapper["node"]
                tier = bool(coverage["isClosed"])
                # GraphQL hands back relay ids (`CoverageNode:<uuid>`);
                # every mutation wants the bare UUID, and the prefixed form
                # fails with `nao e um UUID valido`. `_strip_id` is the same
                # helper the MCP's own read tools use, so there is one
                # normalisation here, not a second implementation of it.
                ranges = [
                    bd_mcp_metadata._strip_id(r["node"]["id"])
                    for r in coverage["datetimeRanges"]["edges"]
                ]
                if tier in tiers:
                    tiers[tier]["extra_coverage_ids"].append(
                        bd_mcp_metadata._strip_id(coverage["id"])
                    )
                    tiers[tier]["extra_range_ids"].extend(ranges)
                    continue
                tiers[tier] = {
                    "id": bd_mcp_metadata._strip_id(coverage["id"]),
                    "range_ids": ranges,
                    "extra_coverage_ids": [],
                    "extra_range_ids": [],
                }
            out[table["slug"]] = tiers
    return out


@dataclass(frozen=True)
class CoveragePlan:
    """What to write for one table's coverage. Pure: decided, not issued.

    Kept separate from the writing so it can be checked without a backend --
    see check_coverage_tiers.py. Every field answers one of the ways the earlier
    position-based code damaged production.
    """

    #: id of the free Coverage to update, or None to create it.
    free_coverage_id: str | None
    #: id of the free DateTimeRange to update, or None to create it.
    free_range_id: str | None
    #: whether to write the declared range at all. False for a table whose
    #: range the flow recomputes every run and already has one.
    write_range: bool
    #: whether to create the pro Coverage (part_bdpro table that lacks one).
    create_pro_coverage: bool
    note: str = ""


def coverage_plan(
    table: str, tiers: dict[bool, dict[str, Any]]
) -> CoveragePlan:
    """Decide the coverage writes for `table` given what the backend holds.

    Two rules, each protecting a paywall the old code broke:

    * The free Coverage is found by `is_closed`, never by position. On prod
      `ata_registro_preco_item` lists the PRO coverage first, so `coverages[0]`
      overwrote the BD Pro window with the free range.
    * A range the flow owns is never restated from the static literal. The flow
      recomputes it on every run, day-granular and rolling (free ends
      2026-03-24, pro starts 2026-03-25); the month-granular literal from
      table_metadata.py would coarsen that boundary and move it forward,
      releasing the paywalled window for free.

      For the paid tier that holds even when the range is **absent**, so a new
      part_bdpro table gets its two Coverages and no range at all. Seeding
      `contratacao` as free through 2026-07 would declare the whole paid window
      open until the first materialisation corrected it. A fully free table can
      be seeded safely -- the flow owns that range too, but a coarser literal
      over all-free data misclassifies nothing -- and is, so a correction to
      `table_metadata.py` still lands on the tables no flow covers.
    """
    free = tiers.get(False)
    free_range_id = (
        free["range_ids"][0] if free and free["range_ids"] else None
    )
    # A table the flow covers has a spec; `PartBdpro` is the paid tier, the
    # same predicate `policy.needs_row_access_policy` uses.
    spec = COVERAGE.get(table)
    paid = isinstance(spec, PartBdpro)
    keep = paid or (spec is not None and free_range_id is not None)
    create_pro = paid and tiers.get(True) is None
    notes = []
    if paid:
        notes.append("range left to the flow (paid tier)")
    elif keep:
        notes.append("range kept (pipeline-owned)")
    if create_pro:
        notes.append("pro coverage created")
    return CoveragePlan(
        free_coverage_id=free["id"] if free else None,
        free_range_id=free_range_id,
        write_range=not keep,
        create_pro_coverage=create_pro,
        note=", ".join(notes),
    )


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
    """Index a table node's child records so their ids can be reused.

    Keeps the FIRST record for a repeated entity, matching what `prune` keeps.
    A dict comprehension keeps the last, which would hand back the id `prune`
    had just deleted -- latent today only because `prune` is inert (the backend
    exposes no delete tool), and silently wrong the moment one is added.
    """
    levels: dict[Any, Any] = {}
    for level in node.get("observation_levels", []):
        levels.setdefault(level.get("entity_id"), level["id"])
    # Coverages and datetime ranges are deliberately absent: they are read by
    # tier through `stored_coverages`, because `get_dataset` exposes neither
    # `is_closed` nor a stable coverage order, and a positional id there is what
    # flattened the free/pro pair.
    return {
        "observation_levels": levels,
        "cloud_tables": [c["id"] for c in node.get("cloud_tables", [])],
        "updates": [u["id"] for u in node.get("updates", [])],
    }


def prune(
    node: dict[str, Any],
    tiers: dict[bool, dict[str, Any]],
    env: str,
) -> None:
    """Delete duplicate child records left by earlier non-idempotent runs.

    Duplicate coverages are not merely untidy: they make a later
    create_update_table fail with an error that names `coverages_areas`, a field
    that appears nowhere in the request.

    Coverages and their ranges are deduplicated **within a tier**. Deleting
    `coverages[1:]` outright, as this did, removes the BD Pro coverage from every
    part_bdpro table -- it is a legitimate second coverage, not a duplicate --
    and `assert_coverage_topology` then hard-fails the next pipeline run with
    `part_bdpro exige Coverage free + pro`. Ranges are likewise per coverage: a
    flat `ranges[1:]` across both tiers deletes the pro window's only range.
    """
    doomed: list[tuple[str, str]] = []
    seen: set[Any] = set()
    for level in node.get("observation_levels", []):
        key = level.get("entity_id")
        if key in seen:
            doomed.append(("observationlevel", level["id"]))
        seen.add(key)
    for extra in node.get("updates", [])[1:]:
        doomed.append(("update", extra["id"]))
    for tier in tiers.values():
        for record_id in tier["extra_coverage_ids"]:
            doomed.append(("coverage", record_id))
        for record_id in tier["extra_range_ids"] + tier["range_ids"][1:]:
            doomed.append(("datetimerange", record_id))
    if not doomed:
        return
    # The caller clears the extra ids straight after this returns, on the
    # premise that they are gone. Without a delete tool they would not be, and
    # the run would carry on to fail at create_update_table with an error that
    # names `coverages_areas` -- a field the request does not contain. Stop
    # here instead, naming what has to go.
    if not hasattr(bd_mcp_write, "delete_record"):
        print(
            f"{len(doomed)} duplicate child record(s) on this table and the "
            f"MCP server has no delete_record: {doomed}"
        )
        print(
            "Merge basedosdados/mcp#13 (or delete them in Django admin) and "
            "re-run; proceeding would fail at create_update_table."
        )
        sys.exit(1)
    delete = fn("delete_record")
    for kind, record_id in doomed:
        delete(kind=kind, record_id=record_id, env=env)


def main(env: str, status: str, only: list[str] | None = None) -> int:
    """Register the dataset's metadata.

    `only` restricts the run to the named tables -- a convenience for re-running
    one table, not a safety measure. It used to be the latter: prod carries a
    second, `is_closed=True` Coverage on every part_bdpro table (the BD Pro
    window) that this script knew nothing about, so an unscoped prod run
    flattened the free/pro pair and left `assert_coverage_topology` failing. The
    tiers are now read by `is_closed` and the pipeline-owned ranges left alone,
    so an unscoped run is safe; `only` no longer carries that weight.
    """
    targets = [t for t in TABLE_ORDER if not only or t in only]
    if only:
        unknown = sorted(set(only) - set(TABLE_ORDER))
        if unknown:
            print(f"unknown tables: {unknown}")
            return 1
        print(
            f"scoped to {len(targets)} of {len(TABLE_ORDER)} tables: {targets}"
        )
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
    tag_slugs = DATASET["tags_prod"] if env == "prod" else DATASET["tags"]
    tag_ids = [lookup("tag", slug, env) for slug in tag_slugs]
    if not all([account_id, status_id, org_id, license_id, area_id]):
        print("could not resolve a required reference id")
        return 1
    missing_tags = [
        s for s, i in zip(tag_slugs, tag_ids, strict=True) if not i
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
    existing_sources = {
        candidate.get("url"): candidate["id"]
        for candidate in (
            sources
            if isinstance(sources, list)
            else sources.get("raw_data_sources", [])
        )
    }
    source_ids: dict[str, str] = {}
    for spec_source in (RAW_SOURCE, COMPRASNET_SOURCE):
        # pyrefly: ignore [unsupported-operation]
        source_ids[spec_source["url"]] = fn("create_update_raw_data_source")(
            id=existing_sources.get(spec_source["url"]),
            dataset_id=dataset_id,
            name_pt=spec_source["name_pt"],
            name_en=spec_source["name_en"],
            name_es=spec_source["name_es"],
            description_pt=spec_source["description_pt"],
            description_en=spec_source["description_en"],
            description_es=spec_source["description_es"],
            url=spec_source["url"],
            availability_id=availability_id,
            license_id=license_id,
            # No area_ids: a raw data source carries no geographic coverage in
            # this backend, unlike a table. Passing it is a TypeError, not a
            # no-op.
            contains_api=spec_source.get("contains_api", True),
            is_free=True,
            requires_registration=False,
            status_id=status_id,
            env=env,
        )["id"]
        print(
            # pyrefly: ignore [bad-index]
            f"raw data source {spec_source['url']} -> {source_ids[spec_source['url']]}"
        )

    published_status = lookup("status", "published", env)
    node = fn("get_dataset")(slug=DATASET["slug"], env=env)
    kept_latest = stored_update_latest(DATASET["slug"], env)
    coverage_tiers = stored_coverages(DATASET["slug"], env)
    table_ids: dict[str, str] = {}

    for table in targets:
        meta = META[table]
        spec = DBT[table]
        current = node.get("tables", {}).get(table, {})
        tiers = coverage_tiers.get(table, {})
        prune(current, tiers, env)
        prior = existing(current)
        coverage_note = ""
        # prune deleted the extras; what it kept is what the ids below reuse.
        for tier in tiers.values():
            tier["extra_coverage_ids"] = []
            tier["extra_range_ids"] = []
            tier["range_ids"] = tier["range_ids"][:1]

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
            # The source is created above, before this loop, so it can be linked
            # here rather than in a deferred second pass. Without it the tables
            # ship with no raw data source at all -- which is how the first prod
            # registration of the two ComprasNet tables ended up with an empty
            # rawDataSource while staging had it, set by hand months earlier.
            raw_data_source_ids=[
                source_ids[
                    COMPRASNET_SOURCE["url"]
                    if table in COMPRASNET_TABLES
                    else RAW_SOURCE["url"]
                ]
            ],
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
            plan = coverage_plan(table, tiers)
            coverage_note = plan.note
            coverage_id = fn("create_update_coverage")(
                id=plan.free_coverage_id,
                table_id=table_id,
                area_id=area_id,
                is_closed=False,
                env=env,
            )["id"]
            if plan.write_range:
                start_year, start_month, end_year, end_month = meta.coverage
                fn("create_update_datetime_range")(
                    id=plan.free_range_id,
                    coverage_id=coverage_id,
                    start_year=start_year,
                    start_month=start_month,
                    end_year=end_year,
                    end_month=end_month,
                    interval=1,
                    is_closed=False,
                    env=env,
                )
            # A part_bdpro table needs its pro Coverage to EXIST, or the next
            # pipeline run dies at assert_coverage_topology. It needs no range
            # here: the flow writes both from the real max date on every run.
            # An AllFree table must have NO pro coverage, so this never fires
            # outside the PartBdpro specs.
            if plan.create_pro_coverage:
                fn("create_update_coverage")(
                    id=None,
                    table_id=table_id,
                    area_id=area_id,
                    is_closed=True,
                    env=env,
                )

        # The table-anchored Update: when WE last refreshed the table, and how
        # often we do. `latest` is a wall clock, per the convention -- the
        # source's own publication date belongs on a source-anchored Update.
        entity_slug, frequency = UPDATE_CADENCE[table]
        cadence_entity = lookup("entity", entity_slug, env)
        if cadence_entity is None:
            sys.exit(f"{table}: entity {entity_slug!r} not found in {env}")
        fn("create_update_update")(
            id=prior["updates"][0] if prior["updates"] else None,
            table_id=table_id,
            entity_id=cadence_entity,
            frequency=frequency,
            latest=kept_latest.get(table, REFRESHED_AT),
            env=env,
        )

        counts = result if isinstance(result, dict) else {}
        print(
            f"  {table:<28} id={table_id[:8]}… columns="
            f"{counts.get('created', '?')}+{counts.get('updated', '?')} "
            f"levels={len(level_ids)}"
            + (f" coverage={coverage_note}" if coverage_note else "")
        )

    # reorder_tables keys on the dataset SLUG, not its id. Skipped on a scoped
    # run: it would restate the order of tables this run was told not to touch.
    if not only:
        fn("reorder_tables")(
            dataset_slug=DATASET["slug"], table_slugs=TABLE_ORDER, env=env
        )
    print(f"\nregistered {len(table_ids)} tables in {env}")
    return 0


if __name__ == "__main__":
    environment = sys.argv[1] if len(sys.argv) > 1 else "staging"
    dataset_status = sys.argv[2] if len(sys.argv) > 2 else "under_review"
    # Any further arguments name the only tables to register.
    raise SystemExit(main(environment, dataset_status, sys.argv[3:] or None))
