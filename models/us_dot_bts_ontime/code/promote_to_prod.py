"""Copy the verified staging metadata for us_dot_bts_ontime to the prod backend.

    uv run --no-project --python 3.11 --with fastmcp --with requests \
        python models/us_dot_bts_ontime/code/promote_to_prod.py

Reads each table's descriptions back out of staging and writes them to prod, so
the two cannot drift through a transcription slip. Only IDs are restated here,
because they genuinely differ between backends.

Idempotency matters: `create_update_*` creates a duplicate when called without an
`id`, and two coverages on one table then break `CreateUpdateTable` outright with
"no field named coverages_areas". Every object this script creates is looked up
first and updated in place if it already exists, so a re-run is safe.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from gen_columns_json import payload  # noqa: E402

MCP_SERVER = Path.home() / "Dropbox" / "BD" / "mcp" / "server.py"

STAGING_TABLES = {
    "flight": "245b6498-5295-44df-8f4d-62496f2ba898",
    "airport": "2a0d9769-c080-4a84-a77e-016996a3fae8",
    "dicionario": "fe828676-962b-4354-9605-fb83acd2b3b3",
}

PROD = {
    "dataset": "3f7c078a-008e-407f-bbc7-45153d9a9b5e",
    "account": "4",
    "status_under_review": "47208305-325a-4da9-9222-ac6849405b78",
    "status_published": "e16221de-ac30-4926-83d3-de219998dab3",
    "area_us": "61a2c232-c649-4b41-a5a3-1467b7393e11",
    "entity_flight": "7902d189-c638-41f8-9f82-27a52964cdb5",
    "entity_airport": "fafdaceb-4b59-4349-a510-c9615f77c0d3",
    "entity_year": "e1bf146e-b6bb-4b65-bee7-c800876e80a5",
    "entity_month": "f9659fea-e9bb-4177-9ca0-54076a8c0932",
    "raw_ontime": "20fdfeac-a0c7-4f78-a640-ff1ba50fe6f7",
    "raw_lookup": "2d81ea46-c467-4eeb-99db-10ca16d28fbd",
}

RAW_SOURCE = {
    "flight": "raw_ontime",
    "airport": "raw_lookup",
    "dicionario": "raw_lookup",
}

# Prod cloud tables point at the prod project. The tables do not exist there yet:
# they are materialized by the GitHub table-approve action when the PR merges.
GCP_PROJECT = "basedosdados"
GCP_DATASET = "us_dot_bts_ontime"

# The auxiliary bundles must be copied to the prod bucket by someone holding prod
# credentials; a dev service account is refused. The URL is registered at its
# correct final location rather than pointing prod metadata at a dev bucket.
AUX = (
    "https://storage.googleapis.com/basedosdados/auxiliary_files/"
    "us_dot_bts_ontime/{table}/auxiliary_files.zip"
)

# Grain columns per observation level, matching what was linked on staging.
OL_COLUMNS = {
    "flight": {
        "entity_flight": [
            "flight_date",
            "reporting_carrier",
            "flight_number",
            "origin",
        ],
        "entity_year": ["year"],
    },
    "airport": {"entity_airport": ["airport_id"]},
}
PARTITION = {"flight": "year"}


def load_mcp():
    spec = importlib.util.spec_from_file_location(
        "databasis_mcp_server", MCP_SERVER
    )
    if spec is None or spec.loader is None:
        raise SystemExit(f"cannot import the databasis MCP at {MCP_SERVER}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def call(tool):
    return getattr(tool, "fn", tool)


def bare(gid: str) -> str:
    return gid.split(":")[-1]


def staging_table(mcp, table_id: str) -> dict:
    q = """query($id: ID!) { allTable(id: $id) { edges { node {
      slug namePt nameEn nameEs descriptionPt descriptionEn descriptionEs
    } } } }"""
    return mcp._gql(q, {"id": table_id}, env="staging")["allTable"]["edges"][
        0
    ]["node"]


def prod_tables(mcp) -> dict[str, dict]:
    q = """query($id: ID!) { allTable(dataset_Id: $id, first: 100) { edges { node {
      id slug
      observationLevels { edges { node { id entity { id } } } }
      cloudTables { edges { node { id } } }
      coverages { edges { node { id isClosed datetimeRanges { edges { node { id } } } } } }
      updates { edges { node { id } } }
    } } } }"""
    out = {}
    data = mcp._gql(q, {"id": PROD["dataset"]}, env="prod")
    for e in data["allTable"]["edges"]:
        n = e["node"]
        out[n["slug"]] = n
    return out


def column_ids(mcp, table_id: str) -> dict[str, str]:
    q = """query($id: ID!) { allColumn(table_Id: $id, first: 1000) {
      edges { node { id name } } } }"""
    data = mcp._gql(q, {"id": table_id}, env="prod")
    return {
        e["node"]["name"]: bare(e["node"]["id"])
        for e in data["allColumn"]["edges"]
    }


def main() -> None:
    mcp = load_mcp()
    call(mcp.auth)(env="prod")
    call(mcp.auth)(env="staging")

    existing = prod_tables(mcp)

    for slug, staging_id in STAGING_TABLES.items():
        src = staging_table(mcp, staging_id)
        prior = existing.get(slug)
        tid = bare(prior["id"]) if prior else None

        res = call(mcp.create_update_table)(
            id=tid,
            slug=slug,
            name_pt=src["namePt"],
            name_en=src["nameEn"],
            name_es=src["nameEs"],
            description_pt=src["descriptionPt"],
            description_en=src["descriptionEn"],
            description_es=src["descriptionEs"],
            dataset_id=PROD["dataset"],
            status_id=PROD["status_published"],
            published_by_ids=[PROD["account"]],
            data_cleaned_by_ids=[PROD["account"]],
            raw_data_source_ids=[PROD[RAW_SOURCE[slug]]],
            auxiliary_files_url=AUX.format(table=slug),
            env="prod",
        )
        tid = res["id"]
        print(f"\n=== {slug}: table {tid}")

        r = call(mcp.bulk_upsert_columns)(
            table_id=tid, columns_json=payload(slug), env="prod"
        )
        print(
            f"    columns created={r.get('created')} updated={r.get('updated')}"
        )

        # Observation levels, then the per-column FK that makes the site show the
        # level's columns instead of "Não informado".
        ol_ids = {}
        prior_ols = {
            bare(e["node"]["entity"]["id"]): bare(e["node"]["id"])
            for e in (prior["observationLevels"]["edges"] if prior else [])
        }
        for entity_key in OL_COLUMNS.get(slug, {}):
            eid = PROD[entity_key]
            ol = call(mcp.create_update_observation_level)(
                id=prior_ols.get(eid), table_id=tid, entity_id=eid, env="prod"
            )
            ol_ids[entity_key] = ol["id"]
            print(f"    OL {entity_key} -> {ol['id']}")

        if ol_ids:
            cids = column_ids(mcp, tid)
            for entity_key, cols in OL_COLUMNS[slug].items():
                for name in cols:
                    call(mcp.update_column)(
                        column_id=cids[name],
                        column_name=name,
                        table_id=tid,
                        observation_level_id=ol_ids[entity_key],
                        is_partition=(name == PARTITION.get(slug)),
                        env="prod",
                    )
            print(
                f"    linked {sum(len(v) for v in OL_COLUMNS[slug].values())} columns"
            )

        ct_prior = [
            bare(e["node"]["id"])
            for e in (prior["cloudTables"]["edges"] if prior else [])
        ]
        ct = call(mcp.create_update_cloud_table)(
            id=ct_prior[0] if ct_prior else None,
            table_id=tid,
            gcp_project_id=GCP_PROJECT,
            gcp_dataset_id=GCP_DATASET,
            gcp_table_id=slug,
            env="prod",
        )
        print(
            f"    cloud table {ct['id']} -> {GCP_PROJECT}.{GCP_DATASET}.{slug}"
        )

    print(
        "\nprod metadata written; dataset stays under_review until the PR merges"
    )


if __name__ == "__main__":
    main()
