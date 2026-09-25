"""Register br_mj_sinesp metadata in the Data Basis backend.

    python register_metadata.py staging
    python register_metadata.py prod     # only after the checkpoint is approved

Calls the databasis MCP tool functions directly — they are plain functions — so
the column payloads are read from the architecture CSVs instead of being pasted
through a tool call. Every create_update_* is passed the id of the record it
should update when one already exists, because several of those endpoints
duplicate silently on a re-run without one.
"""

from __future__ import annotations

import contextlib
import csv
import json
import os
import sys

sys.path.insert(
    0,
    os.path.expanduser("~/Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp"),
)
import server

HERE = os.path.dirname(os.path.abspath(__file__))
ARCH = os.path.join(HERE, "architecture")
with open(os.path.join(HERE, "metadata.json"), encoding="utf-8") as f:
    META = json.load(f)
with open(os.path.join(ARCH, "translations.json"), encoding="utf-8") as f:
    TRANS = json.load(f)

GCP_PROJECT = {
    "staging": "basedosdados-dev",
    "dev": "basedosdados-dev",
    "prod": "basedosdados",
}
PARTITIONS = {
    "municipio_mes": {"ano", "sigla_uf"},
    "uf_mes": {"ano", "sigla_uf"},
    "dicionario": set(),
}
# Grain columns, so the site does not render each level's columns as
# "Não informado". bulk_upsert_columns cannot set these; update_column must.
OL_COLUMNS = {
    "municipio_mes": {
        "ano": "year",
        "mes": "month",
        "id_municipio": "municipality",
    },
    "uf_mes": {"ano": "year", "mes": "month", "sigla_uf": "state"},
    "dicionario": {},
}
# The table-anchored Update.latest is when WE last refreshed -- a wall clock,
# not a coverage date. The backend field is a DateTime, not a Date.
REFRESHED_AT = "2026-09-24T00:00:00+00:00"
COVERAGE = {
    "municipio_mes": (2015, 1, 2026, 8),
    "uf_mes": (2015, 1, 2026, 8),
    "dicionario": None,
}


def arch_rows(table: str) -> list[dict]:
    with open(os.path.join(ARCH, f"{table}.csv"), encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def columns_payload(table: str) -> list[dict]:
    out = []
    for r in arch_rows(table):
        t = TRANS[r["name"]]
        out.append(
            {
                "name": r["name"],
                "bigquery_type": r["bigquery_type"],
                "description_pt": r["description"],
                "description_en": t["en"],
                "description_es": t["es"],
                "covered_by_dictionary": r["covered_by_dictionary"] == "yes",
                "directory_column": r["directory_column"] or None,
                "measurement_unit": r["measurement_unit"] or None,
                "has_sensitive_data": r["has_sensitive_data"] == "yes",
                "observations": r["observations"] or None,
                "is_partition": r["name"] in PARTITIONS[table],
            }
        )
    return out


def column_ids(table_id: str, env: str) -> dict[str, str]:
    q = """query($t: ID){ allColumn(table_Id: $t, first: 200){
      edges{ node{ id name } } } }"""
    r = server._gql(q, {"t": table_id}, env=env)
    return {
        e["node"]["name"]: server._strip_id(e["node"]["id"])
        for e in r["allColumn"]["edges"]
    }


def table_ids(dataset_id: str, env: str) -> dict[str, str]:
    """Existing tables by slug, so a re-run updates instead of colliding."""
    q = """query($d: ID){ allTable(dataset_Id: $d, first: 100){
      edges{ node{ id slug } } } }"""
    r = server._gql(q, {"d": dataset_id}, env=env)
    return {
        e["node"]["slug"]: server._strip_id(e["node"]["id"])
        for e in r["allTable"]["edges"]
    }


def existing_by_slug(parent_id: str, env: str, query: str, key: str) -> dict:
    r = server._gql(query, {"p": parent_id}, env=env)
    return {
        e["node"]["slug"]: server._strip_id(e["node"]["id"])
        for e in r[key]["edges"]
    }


def existing_coverage(
    table_id: str, env: str
) -> tuple[str | None, str | None]:
    """The table's free coverage and its datetime range, if already present."""
    q = """query($t: ID){ allTable(id: $t, first: 1){ edges{ node{
      coverages{ edges{ node{ id isClosed
        datetimeRanges{ edges{ node{ id } } } } } } } } } }"""
    r = server._gql(q, {"t": table_id}, env=env)
    for e in r["allTable"]["edges"]:
        for c in e["node"]["coverages"]["edges"]:
            if c["node"]["isClosed"]:
                continue
            drs = c["node"]["datetimeRanges"]["edges"]
            return (
                server._strip_id(c["node"]["id"]),
                server._strip_id(drs[0]["node"]["id"]) if drs else None,
            )
    return None, None


def existing_cloud_table(table_id: str, env: str) -> str | None:
    q = """query($t: ID){ allCloudtable(table_Id: $t, first: 5){
      edges{ node{ id } } } }"""
    try:
        edges = server._gql(q, {"t": table_id}, env=env)["allCloudtable"][
            "edges"
        ]
        return server._strip_id(edges[0]["node"]["id"]) if edges else None
    except Exception:
        return None


def existing_update(table_id: str, env: str) -> str | None:
    q = """query($t: ID){ allUpdate(table_Id: $t, first: 5){
      edges{ node{ id } } } }"""
    try:
        r = server._gql(q, {"t": table_id}, env=env)
        edges = r["allUpdate"]["edges"]
        return server._strip_id(edges[0]["node"]["id"]) if edges else None
    except Exception:
        return None


def main(env: str) -> None:
    d = META["dataset"]

    def lk(cat: str, slug: str) -> str:
        return server.lookup_id(category=cat, slug=slug, env=env)["id"]

    org = lk("organization", d["organization_slug"])
    themes = [lk("theme", s) for s in d["theme_slugs"]]
    tags = []
    for s in d["tag_slugs"]:
        try:
            tags.append(lk("tag", s))
        except Exception as exc:
            print(f"  ! tag {s!r} unresolved in {env}: {str(exc)[:70]}")
    under_review = lk("status", "under_review")
    published = lk("status", "published")
    area_br = lk("area", "br")
    account = server.get_authenticated_account(env=env)
    acc_id = account["id"] if isinstance(account, dict) else account
    print(
        f"org={org}  themes={len(themes)}  tags={len(tags)}  account={acc_id}"
    )

    existing = None
    with contextlib.suppress(Exception):
        existing = server.get_dataset(slug=d["slug"], env=env)
    ds_id = (existing or {}).get("id")

    ds = server.create_update_dataset(
        slug=d["slug"],
        name_pt=d["name_pt"],
        name_en=d["name_en"],
        name_es=d["name_es"],
        description_pt=d["description_pt"],
        description_en=d["description_en"],
        description_es=d["description_es"],
        organization_ids=[org],
        theme_ids=themes,
        tag_ids=tags,
        status_id=under_review,
        id=ds_id,
        env=env,
    )
    ds_id = ds["id"]
    print(f"dataset {d['slug']} -> {ds_id}")

    r = META["raw_data_source"]
    rds_existing = server.get_raw_data_sources(dataset_slug=d["slug"], env=env)
    if isinstance(rds_existing, dict):
        rds_existing = rds_existing.get("raw_data_sources", [])
    rds_id = None
    for node in rds_existing or []:
        if isinstance(node, dict) and node.get("url") == r["url"]:
            rds_id = node.get("id")
    rds = server.create_update_raw_data_source(
        dataset_id=ds_id,
        name_pt=r["name_pt"],
        name_en=r["name_en"],
        name_es=r["name_es"],
        url=r["url"],
        license_id=lk("license", "odbl"),
        availability_id=lk("availability", "online"),
        description_pt=r["description_pt"],
        description_en=r["description_en"],
        description_es=r["description_es"],
        has_structured_data=True,
        is_free=True,
        contains_api=False,
        requires_registration=False,
        id=rds_id,
        env=env,
    )
    print(f"raw data source -> {rds['id']}")

    known_tables = table_ids(ds_id, env)

    # PASS 1 -- every create_update_table, including the raw-source link, BEFORE
    # any coverage exists. On staging, CreateUpdateTable fails with "'TableForm'
    # has no field named 'coverages_areas'" as soon as the table has one
    # coverage, so a table can never be updated again afterwards. Confirmed
    # still broken on staging 2026-09-24; reported fixed on prod 2026-09-11.
    tids: dict[str, str] = {}
    for slug, t in META["tables"].items():
        tbl = server.create_update_table(
            id=known_tables.get(slug),
            slug=slug,
            name_pt=t["name_pt"],
            name_en=t["name_en"],
            name_es=t["name_es"],
            dataset_id=ds_id,
            status_id=published,
            published_by_ids=[acc_id],
            data_cleaned_by_ids=[acc_id],
            description_pt=t["description_pt"],
            description_en=t["description_en"],
            description_es=t["description_es"],
            raw_data_source_ids=[rds["id"]],
            env=env,
        )
        tids[slug] = tbl["id"]
        print(f"table {slug} -> {tbl['id']} (raw source linked)")

    # PASS 2 -- everything that hangs off a table.
    for slug, t in META["tables"].items():
        tid = tids[slug]
        print(f"\n{slug}")

        ols = {}
        for ent in t["observation_levels"]:
            ol = server.create_update_observation_level(
                table_id=tid, entity_id=lk("entity", ent), env=env
            )
            ols[ent] = ol["id"]
        print(f"  observation levels: {list(ols)}")

        cols = columns_payload(slug)
        res = server.bulk_upsert_columns(
            table_id=tid,
            columns_json=json.dumps(cols, ensure_ascii=False),
            env=env,
        )
        print(
            f"  columns: {res.get('created', '?')} created / "
            f"{res.get('updated', '?')} updated of {len(cols)}"
        )

        # Link grain columns to their observation level, or the site renders the
        # level's columns as "Não informado". bulk_upsert_columns cannot do it,
        # and update_column's booleans default to False, so is_partition has to
        # be re-passed here or it is clobbered.
        by_name = column_ids(tid, env)
        for col, ent in OL_COLUMNS[slug].items():
            cid = by_name.get(col)
            if not cid or ent not in ols:
                print(f"  ! cannot link {col} -> {ent}")
                continue
            server.update_column(
                column_id=cid,
                column_name=col,
                table_id=tid,
                observation_level_id=ols[ent],
                is_partition=col in PARTITIONS[slug],
                env=env,
            )
        if OL_COLUMNS[slug]:
            print(f"  linked {len(OL_COLUMNS[slug])} grain columns")

        # bulk_upsert_columns does NOT set is_partition, so any partition
        # column the observation-level pass did not touch still needs it.
        # municipio_mes.sigla_uf is exactly that case: it is a partition but
        # not the grain column of any observation level.
        for col in PARTITIONS[slug] - set(OL_COLUMNS[slug]):
            cid = by_name.get(col)
            if cid:
                server.update_column(
                    column_id=cid,
                    column_name=col,
                    table_id=tid,
                    is_partition=True,
                    env=env,
                )
                print(f"  partition flag set on {col}")

        # create_update_cloud_table duplicates without an id, so a re-run
        # otherwise leaves several cloud tables on the same BigQuery table.
        server.create_update_cloud_table(
            table_id=tid,
            gcp_project_id=GCP_PROJECT[env],
            gcp_dataset_id="br_mj_sinesp",
            gcp_table_id=slug,
            id=existing_cloud_table(tid, env),
            env=env,
        )

        # A table with no date column takes no coverage at all; creating one
        # leaves an empty coverage the frontend cannot render. Reuse the
        # existing coverage rather than adding a second: create_update_coverage
        # is not idempotent without an id either.
        if COVERAGE[slug]:
            cov_id, range_id = existing_coverage(tid, env)
            cov = server.create_update_coverage(
                table_id=tid,
                area_id=area_br,
                is_closed=False,
                id=cov_id,
                env=env,
            )
            y0, m0, y1, m1 = COVERAGE[slug]
            server.create_update_datetime_range(
                coverage_id=cov["id"],
                start_year=y0,
                start_month=m0,
                end_year=y1,
                end_month=m1,
                interval=1,
                id=range_id,
                env=env,
            )
            print(f"  coverage {y0}-{m0:02d} .. {y1}-{m1:02d}")

        # Table-anchored Update.latest is when WE last refreshed: a wall clock,
        # not a coverage date.
        server.create_update_update(
            table_id=tid,
            entity_id=lk("entity", "month"),
            frequency=1,
            lag=1,
            latest=REFRESHED_AT,
            id=existing_update(tid, env),
            env=env,
        )

    server.reorder_tables(
        dataset_slug=d["slug"], table_slugs=list(META["tables"]), env=env
    )

    # Step 9b: publish on dev/staging only, so the reviewer sees the dataset as
    # it will appear. The production dataset stays under_review until the PR is
    # merged, table-approve has materialised the prod tables, and both are
    # verified -- that flip is a separate, deliberate post-merge action.
    if env in ("dev", "staging"):
        server.create_update_dataset(
            slug=d["slug"],
            name_pt=d["name_pt"],
            name_en=d["name_en"],
            name_es=d["name_es"],
            description_pt=d["description_pt"],
            description_en=d["description_en"],
            description_es=d["description_es"],
            organization_ids=[org],
            theme_ids=themes,
            tag_ids=tags,
            status_id=published,
            id=ds_id,
            env=env,
        )
        print(f"dataset published on {env}")
    print(f"\ndone. dataset {d['slug']} ({ds_id}) registered in {env}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "staging")
