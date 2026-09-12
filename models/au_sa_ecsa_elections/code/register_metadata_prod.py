"""Promote the dataset's metadata to the production backend.

The staging registration lives in ``register_metadata.py``; this is its prod
counterpart. It is a separate file rather than an ``--env`` flag because almost
every reference id differs between the two backends, and copying a staging UUID
across is the failure this file exists to prevent: the organization slug drops
its ``au_`` prefix, area UUIDs are not shared, the account is different, and the
tag vocabulary is spelled in English with one tag whose UUID also differs. Every
id below is resolved against prod at run time, by slug.

The dataset is registered ``under_review``. It is published only after the
onboarding PR merges, the table-approve action materialises
``basedosdados.au_sa_ecsa_elections.*`` and those tables are verified — a
separate, later action. The cloud tables registered here point at prod tables
that do not exist yet, which is expected at this step.

Licensing is deliberate and must not be "upgraded". Both raw data sources carry
``unknown``. The ECSA copyright page grants Creative Commons Australia
Attribution 3.0 over ``ecsa.sa.gov.au``, but that grant does not reach the two
hosts the data actually comes from: the results API publishes no licence
statement of any kind, and the funding disclosure portals render an all rights
reserved footer. The dataset description states this in all three languages.

Idempotent: every record is read before it is written and its id passed back,
because create_update_cloud_table, create_update_observation_level,
create_update_coverage, create_update_datetime_range and create_update_update
match on no natural key and silently create a duplicate when called without one.
A second unguarded pass on staging duplicated 55 child records.

Ordering is not the obvious one. ``create_update_table`` fails on a table that
already has a Coverage, so every table write — the raw data source link included
— happens before the first coverage is created.

Usage::

    PYTHONPATH=. python models/au_sa_ecsa_elections/code/register_metadata_prod.py
"""

from __future__ import annotations

import json
import pathlib
import sys
import time
from collections.abc import Callable
from typing import Any

MCP = (
    pathlib.Path.home() / "Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp"
)
sys.path.insert(0, str(MCP))

import server  # noqa: E402

ENV = "prod"
DATASET_SLUG = "sa_elections"
GCP_PROJECT = "basedosdados"
GCP_DATASET = "au_sa_ecsa_elections"

META = pathlib.Path(__file__).resolve().parent / "metadata"

ORG_SLUG = "ecsa"
ORG_WEBSITE = "https://www.ecsa.sa.gov.au/"
THEME_SLUGS = ["government", "politics"]
# Prod spells the vocabulary in English. Seven of the eight keep the UUID they
# have on staging; campaign-finance does not, which is why every tag is resolved
# by slug and the attached set is verified afterwards.
TAG_SLUGS = [
    "campaign-finance",
    "candidacy",
    "donation",
    "elections",
    "political_party",
    "preferences",
    "electoral-system",
    "vote",
]

TABLE_ORDER = [
    "election",
    "candidate",
    "result_district",
    "result_voting_centre",
    "distribution_of_preferences",
    "enrolment_turnout",
    "voting_centre",
    "disclosure_return",
    "dicionario",
]

COVERAGE = {t: (2022, 2026) for t in TABLE_ORDER}
COVERAGE["disclosure_return"] = (2015, 2026)

# When the tables were last refreshed at Data Basis — a wall clock, not a
# coverage date. A bare date is rejected by the backend's DateTime scalar.
LAST_REFRESHED = "2026-09-10T00:00:00"
FREQUENCY = {t: 4 for t in TABLE_ORDER}
FREQUENCY["disclosure_return"] = 1

ORG = {
    "name_pt": "Comissão Eleitoral da Austrália Meridional",
    "name_en": "Electoral Commission South Australia",
    "name_es": "Comisión Electoral de Australia Meridional",
    "description_pt": (
        "Órgão independente responsável pela condução das eleições estaduais e "
        "de governo local da Austrália Meridional e pela fiscalização do "
        "financiamento político no estado."
    ),
    "description_en": (
        "Independent body responsible for conducting South Australian state and "
        "local government elections and for regulating political funding in the "
        "state."
    ),
    "description_es": (
        "Organismo independiente responsable de conducir las elecciones "
        "estatales y de gobierno local de Australia Meridional y de fiscalizar "
        "el financiamiento político en el estado."
    ),
}


def log(message: str) -> None:
    print(message, flush=True)


def transient(error: Exception) -> bool:
    """A 5xx is a served response, not a timeout, and is worth retrying."""
    text = str(error)
    return (
        "HTTP 5" in text
        or "timed out" in text.lower()
        or "Connection" in text
        or "Read timed out" in text
    )


def retry(
    write: Callable[[], Any],
    probe: Callable[[], Any] | None = None,
    tries: int = 4,
) -> Any:
    """Retry a write on a 5xx, checking first whether it already landed.

    Prod latency runs high enough that a gateway can return 504 after the write
    committed. Retrying blindly is how duplicates are made, so the probe runs
    before every retry and short-circuits when the record is already there.
    """
    last: Exception | None = None
    for _attempt in range(tries):
        try:
            return write()
        except Exception as error:
            if not transient(error):
                raise
            last = error
            log(f"      transient: {str(error)[:120]}")
            time.sleep(5)
            if probe is not None:
                landed = probe()
                if landed:
                    log("      the write had landed; reusing it")
                    return landed
    raise RuntimeError(f"gave up after {tries} attempts: {last}")


def resolve() -> dict[str, Any]:
    ids = server.discover_ids(
        env=ENV,
        keys=[
            "status",
            "entity",
            "license",
            "availability",
            "theme",
            "tag",
            "language",
        ],
    )
    entity_slugs = (
        "year",
        "election",
        "district",
        "person",
        "electoral_booth",
        "other",
    )
    out: dict[str, Any] = {
        "status_under_review": ids["status"].get("under_review"),
        "status_published": ids["status"].get("published"),
        "license_unknown": ids["license"].get("unknown"),
        "availability_online": ids["availability"].get("online"),
        "language_en": ids["language"].get("en"),
        "themes": [ids["theme"].get(s) for s in THEME_SLUGS],
        "tags": [ids["tag"].get(s) for s in TAG_SLUGS],
        "entity": {slug: ids["entity"].get(slug) for slug in entity_slugs},
        "area_au_sa": server.lookup_id(category="area", slug="au_sa", env=ENV)[
            "id"
        ],
        "account": server.get_authenticated_account(env=ENV)["id"],
    }
    # Every id is resolved by slug against prod, never copied from staging. A
    # slug spelled differently here resolves to None rather than to the wrong
    # record, and the run stops before it writes anything.
    missing = [k for k, v in out.items() if v is None]
    missing += [
        s for s, v in zip(TAG_SLUGS, out["tags"], strict=True) if v is None
    ]
    missing += [
        s for s, v in zip(THEME_SLUGS, out["themes"], strict=True) if v is None
    ]
    missing += [s for s in entity_slugs if out["entity"][s] is None]
    if missing:
        raise SystemExit(f"unresolved reference ids on prod: {missing}")
    # The licence is the point of this promotion: both raw data sources carry
    # 'unknown'. Assert the id rather than trusting the slug lookup silently.
    if out["license_unknown"] != "77dfe32b-6a14-4490-806f-22af1f26c425":
        raise SystemExit(
            f"the 'unknown' licence resolved unexpectedly: {out['license_unknown']}"
        )
    return out


def find_org() -> str | None:
    query = (
        f'{{ allOrganization(slug: "{ORG_SLUG}") '
        "{ edges { node { id } } } }"
    )
    edges = server._gql(query, {}, env=ENV)["allOrganization"]["edges"]
    return server._strip_id(edges[0]["node"]["id"]) if edges else None


def find_raw_sources() -> dict[str, str]:
    query = (
        f'{{ allDataset(slug: "{DATASET_SLUG}") '
        "{ edges { node { rawDataSources { edges { node "
        "{ id url } } } } } } }"
    )
    edges = server._gql(query, {}, env=ENV)["allDataset"]["edges"]
    if not edges:
        return {}
    return {
        e["node"]["url"]: server._strip_id(e["node"]["id"])
        for e in edges[0]["node"]["rawDataSources"]["edges"]
    }


def existing_children(table_id: str) -> dict:
    query = (
        f'{{ allTable(id: "{table_id}") {{ edges {{ node {{ '
        "cloudTables { edges { node { id gcpTableId } } } "
        "observationLevels { edges { node { id entity { slug } } } } "
        "updates { edges { node { id entity { slug } } } } "
        "coverages { edges { node { id isClosed "
        "datetimeRanges { edges { node { id } } } } } } "
        "} } } }"
    )
    edges = server._gql(query, {}, env=ENV)["allTable"]["edges"]
    out: dict[str, Any] = {
        "cloud": {},
        "levels": {},
        "updates": {},
        "coverage": None,
        "range": None,
    }
    if not edges:
        return out
    node = edges[0]["node"]
    for e in node["cloudTables"]["edges"]:
        out["cloud"][e["node"]["gcpTableId"]] = server._strip_id(
            e["node"]["id"]
        )
    for e in node["observationLevels"]["edges"]:
        out["levels"].setdefault(
            e["node"]["entity"]["slug"], server._strip_id(e["node"]["id"])
        )
    for e in node["updates"]["edges"]:
        out["updates"].setdefault(
            e["node"]["entity"]["slug"], server._strip_id(e["node"]["id"])
        )
    for e in node["coverages"]["edges"]:
        if e["node"]["isClosed"]:
            continue
        out["coverage"] = server._strip_id(e["node"]["id"])
        ranges = e["node"]["datetimeRanges"]["edges"]
        if ranges:
            out["range"] = server._strip_id(ranges[0]["node"]["id"])
        break
    return out


def main() -> int:
    ref = resolve()
    log(
        f"resolved prod ids: area_au_sa={ref['area_au_sa']} account={ref['account']}"
    )

    sources_meta = json.loads((META / "raw_data_sources.json").read_text())
    dataset_meta = json.loads((META / "dataset.json").read_text())
    tables_meta = json.loads((META / "tables.json").read_text())

    org_id = retry(
        lambda: server.create_update_organization(
            slug=ORG_SLUG,
            id=find_org(),
            area_id=ref["area_au_sa"],
            website=ORG_WEBSITE,
            env=ENV,
            **ORG,
        )["id"],
        probe=find_org,
    )
    log(f"organization {ORG_SLUG} {org_id}")

    dataset_id = retry(
        lambda: server.create_update_dataset(
            slug=DATASET_SLUG,
            organization_ids=[org_id],
            theme_ids=ref["themes"],
            tag_ids=ref["tags"],
            status_id=ref["status_under_review"],
            id=(server.get_dataset(slug=DATASET_SLUG, env=ENV) or {}).get(
                "id"
            ),
            env=ENV,
            **dataset_meta,
        )["id"],
        probe=lambda: (
            server.get_dataset(slug=DATASET_SLUG, env=ENV) or {}
        ).get("id"),
    )
    log(f"dataset {DATASET_SLUG} {dataset_id} (status under_review)")

    source_ids: dict[str, str] = {}
    for key, meta in sources_meta.items():
        prior = find_raw_sources().get(meta["url"])
        source_ids[key] = retry(
            lambda meta=meta, prior=prior: (
                server.create_update_raw_data_source(
                    dataset_id=dataset_id,
                    license_id=ref["license_unknown"],
                    availability_id=ref["availability_online"],
                    language_ids=[ref["language_en"]],
                    id=prior,
                    env=ENV,
                    **{k: v for k, v in meta.items() if k != "tables"},
                )["id"]
            ),
            probe=lambda meta=meta: find_raw_sources().get(meta["url"]),
        )
        log(f"raw data source {key} {source_ids[key]} licence=unknown")

    table_source = {t: source_ids["results_api"] for t in TABLE_ORDER}
    table_source["disclosure_return"] = source_ids["funding_portals"]

    known = (server.get_dataset(slug=DATASET_SLUG, env=ENV) or {}).get(
        "tables", {}
    ) or {}
    table_ids: dict[str, str] = {}

    # Phase 1 — every table write, the raw source link included. Nothing here may
    # run once a coverage exists.
    for table in TABLE_ORDER:
        meta = tables_meta[table]
        prior = (known.get(table) or {}).get("id")
        table_ids[table] = retry(
            lambda table=table, meta=meta, prior=prior: (
                server.create_update_table(
                    slug=table,
                    name_pt=meta["name_pt"],
                    name_en=meta["name_en"],
                    name_es=meta["name_es"],
                    description_pt=meta["description_pt"],
                    description_en=meta["description_en"],
                    description_es=meta["description_es"],
                    dataset_id=dataset_id,
                    status_id=ref["status_published"],
                    published_by_ids=[ref["account"]],
                    data_cleaned_by_ids=[ref["account"]],
                    raw_data_source_ids=[table_source[table]],
                    id=prior,
                    env=ENV,
                )["id"]
            ),
            probe=lambda table=table: (
                (
                    server.get_dataset(slug=DATASET_SLUG, env=ENV).get(
                        "tables"
                    )
                    or {}
                ).get(table)
                or {}
            ).get("id"),
        )
        log(f"  table {table:30s} {table_ids[table]}")

    # Phase 2 — everything that hangs off a table.
    #
    # The body is a function rather than a loop body because every write below
    # is wrapped in a lambda handed to retry(). A lambda defined in a loop reads
    # the loop variable at call time, not at definition time, so a retry that
    # outlived its iteration would write against the wrong table. Each call
    # binds its own parameters, which removes the hazard rather than papering
    # over it.
    def register_children(table: str, table_id: str) -> None:
        meta = tables_meta[table]
        levels = [tuple(x) for x in meta["observation_levels"]]
        prior_children = existing_children(table_id)

        ol_ids: dict[str, str] = {}
        for entity_slug, _column in levels:
            ol_ids[entity_slug] = retry(
                lambda e=entity_slug: server.create_update_observation_level(
                    table_id=table_id,
                    entity_id=ref["entity"][e],
                    id=prior_children["levels"].get(e),
                    env=ENV,
                )["id"],
                probe=lambda e=entity_slug: existing_children(table_id)[
                    "levels"
                ].get(e),
            )
        if ol_ids:
            retry(
                lambda: server.reorder_observation_levels(
                    table_id=table_id,
                    ol_ids=[ol_ids[e] for e, _ in levels],
                    env=ENV,
                )
            )

        payload = (META / f"{table}.json").read_text()
        result = retry(
            lambda: server.bulk_upsert_columns(
                table_id=table_id, columns_json=payload, env=ENV
            )
        )
        log(f"    {table}: columns {json.dumps(result)[:140]}")

        # bulk_upsert_columns does not link observation levels, so each grain
        # column is linked in its own call. update_column's booleans default to
        # False, so is_partition has to be re-passed or the flag is clobbered.
        column_ids = {
            c["name"]: server._strip_id(c["id"])
            for c in server._fetch_table_columns(table_id, ENV)
        }
        for entity_slug, column_name in levels:
            if column_name is None or column_name not in column_ids:
                continue
            retry(
                lambda c=column_name, e=entity_slug: server.update_column(
                    column_id=column_ids[c],
                    column_name=c,
                    table_id=table_id,
                    observation_level_id=ol_ids[e],
                    is_partition=(c == "year" and table != "dicionario"),
                    env=ENV,
                )
            )
        if (
            table != "dicionario"
            and "year" in column_ids
            and "year" not in {c for _, c in levels}
        ):
            retry(
                lambda: server.update_column(
                    column_id=column_ids["year"],
                    column_name="year",
                    table_id=table_id,
                    is_partition=True,
                    env=ENV,
                )
            )

        retry(
            lambda: server.create_update_cloud_table(
                table_id=table_id,
                gcp_project_id=GCP_PROJECT,
                gcp_dataset_id=GCP_DATASET,
                gcp_table_id=table,
                id=prior_children["cloud"].get(table),
                env=ENV,
            ),
            probe=lambda: existing_children(table_id)["cloud"].get(table),
        )
        coverage_id = retry(
            lambda: server.create_update_coverage(
                table_id=table_id,
                area_id=ref["area_au_sa"],
                id=prior_children["coverage"],
                env=ENV,
            )["id"],
            probe=lambda: existing_children(table_id)["coverage"],
        )
        start, end = COVERAGE[table]
        retry(
            lambda: server.create_update_datetime_range(
                coverage_id=coverage_id,
                start_year=start,
                end_year=end,
                interval=1,
                id=prior_children["range"],
                env=ENV,
            ),
            probe=lambda: existing_children(table_id)["range"],
        )
        retry(
            lambda: server.create_update_update(
                entity_id=ref["entity"]["year"],
                frequency=FREQUENCY[table],
                latest=LAST_REFRESHED,
                table_id=table_id,
                id=prior_children["updates"].get("year"),
                env=ENV,
            ),
            probe=lambda: existing_children(table_id)["updates"].get("year"),
        )
        log(f"    {table}: cloud table, coverage {start}-{end}, update ok")

    for table in TABLE_ORDER:
        register_children(table, table_ids[table])

    retry(
        lambda: server.reorder_tables(
            dataset_slug=DATASET_SLUG, table_slugs=TABLE_ORDER, env=ENV
        )
    )
    log("table order set")
    log(f"DONE dataset={dataset_id} org={org_id} area={ref['area_au_sa']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
