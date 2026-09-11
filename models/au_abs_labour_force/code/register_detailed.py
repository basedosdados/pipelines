#!/usr/bin/env python3
"""Register the 12 Detailed-release tables in the Data Basis backend.

Extends the existing `au_abs_labour_force` dataset record: it adds one raw data
source and twelve tables, refreshes the dataset description and tags for the
extended coverage, and leaves the four Tier-1 tables untouched.

Calls the databasis MCP tools as plain Python functions (see
[[reference_databasis_mcp_direct_call]]) so the 12 column payloads are read from
disk rather than pasted through a conversation.

Idempotent: every record is looked up on the live dataset first and its id
passed back, because `create_update_*` creates a duplicate when the id is
omitted ([[reference_databasis_create_update_not_idempotent]]).

Ordering matters. Every `create_update_table` call — including the raw-source
link — happens before any coverage exists, because the mutation fails on a table
that already has one ([[reference_databasis_create_update_table_coverage_bug]]),
and because a second partial call blanks the table names
([[reference_create_update_table_blanks_names]]).

Usage:
    ~/.venvs/bd-pipelines/bin/python models/au_abs_labour_force/code/register_detailed.py \
        [--env staging|prod] [--publish] [table ...]

Set DATABASIS_MCP_PATH if the databasis MCP checkout is not a sibling of
this repository.
"""

import argparse
import os
import sys
from datetime import date
from pathlib import Path


# The databasis MCP server is a separate checkout, not a dependency of this
# repo, so its location has to come from the environment. DATABASIS_MCP_PATH
# overrides; otherwise fall back to the conventional sibling checkout next to
# this repository. Validated here so a missing checkout fails with a clear
# message instead of an ImportError on `import server`.
def _find_mcp() -> Path:
    """Locate the databasis MCP checkout.

    DATABASIS_MCP_PATH wins. Otherwise walk up from this file looking for a
    sibling ``mcp/server.py`` — which finds it from a normal checkout and from
    a git worktree under ``.claude/worktrees/``, where the repo root sits three
    levels deeper than usual.
    """
    env = os.environ.get("DATABASIS_MCP_PATH")
    if env:
        return Path(env).expanduser()
    for parent in Path(__file__).resolve().parents:
        candidate = parent.parent / "mcp"
        if (candidate / "server.py").is_file():
            return candidate
    return Path("mcp")


_MCP_PATH = _find_mcp()
if not (_MCP_PATH / "server.py").is_file():
    raise SystemExit(
        f"databasis MCP checkout not found at {_MCP_PATH}. Set "
        f"DATABASIS_MCP_PATH to the directory containing server.py."
    )
sys.path.insert(0, str(_MCP_PATH))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import server  # noqa: E402  (import follows the sys.path bootstrap above)
from metadata_detailed import (  # noqa: E402
    DATASET_DESCRIPTION,
    DATASET_SLUG,
    DATASET_TAGS,
    RAW_SOURCE,
    TABLE_META,
    TABLE_ORDER,
)

CODE = Path(__file__).resolve().parent
COLUMNS_JSON = CODE / "columns_json_detailed"
AREA_SLUG = "au"
GCP_PROJECT = {
    "dev": "basedosdados-dev",
    "staging": "basedosdados-dev",
    "prod": "basedosdados",
}
GCP_DATASET = "au_abs_labour_force"

# Existing tags that fit the subject matter, plus the one that has to be created.
NEW_TAGS = {
    "unemployment": ("desemprego", "unemployment", "desempleo"),
}

# The dataset slug and the tag vocabulary differ between backends: staging keeps
# the fully qualified slug and Portuguese tag slugs, while prod uses a short
# English dataset slug and English tag slugs. The underlying records are the
# same — several tags share a UUID across the two backends under different
# slugs — so both must be resolved per environment rather than reused.
# Entities are resolved by slug through discover_ids and need no mapping here,
# which matters because `industry` does NOT share a UUID across the backends.
DATASET_SLUG_BY_ENV = {
    "dev": "au_abs_labour_force",
    "staging": "au_abs_labour_force",
    "prod": "labour_force",
}

DATASET_TAGS_BY_ENV = {
    "prod": [
        "employment",
        "unemployment",
        "labor",
        "occupation",
        "economic-activity",
        "workload",
        "research",
    ],
}


def log(msg: str) -> None:
    print(msg, flush=True)


def partition_columns(table: str) -> set[str]:
    return {"year"}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default="staging")
    ap.add_argument(
        "--publish",
        action="store_true",
        help="flip the dataset to published (dev/staging; prod is always published)",
    )
    ap.add_argument("tables", nargs="*")
    args = ap.parse_args()
    env = args.env
    global DATASET_SLUG, DATASET_TAGS
    DATASET_SLUG = DATASET_SLUG_BY_ENV.get(env, DATASET_SLUG)
    DATASET_TAGS = DATASET_TAGS_BY_ENV.get(env, DATASET_TAGS)
    want = [t for t in TABLE_META if not args.tables or t in set(args.tables)]
    unknown = set(args.tables) - set(TABLE_META)
    if unknown:
        raise SystemExit(f"unknown table(s): {sorted(unknown)}")

    ids = server.discover_ids(
        env=env, keys=["status", "entity", "license", "availability", "tag"]
    )
    status, entity, license_, avail, tag = (
        ids["status"],
        ids["entity"],
        ids["license"],
        ids["availability"],
        ids["tag"],
    )
    area_id = server.lookup_id(category="area", slug=AREA_SLUG, env=env)["id"]
    account = server.get_authenticated_account(env=env)
    account_id = str(account["id"])
    log(f"account={account['email']} area({AREA_SLUG})={area_id}")

    ds = server.get_dataset(DATASET_SLUG, env=env)
    if not ds.get("found"):
        raise SystemExit(f"dataset {DATASET_SLUG} not found on {env}")
    dataset_id = ds["id"]
    log(
        f"dataset {DATASET_SLUG} id={dataset_id}, {len(ds['tables'])} existing tables"
    )

    # ── tags ────────────────────────────────────────────────────────────────
    tag_ids = []
    for slug in DATASET_TAGS:
        if slug in tag:
            tag_ids.append(tag[slug])
        elif slug in NEW_TAGS:
            pt, en, es = NEW_TAGS[slug]
            r = server.create_update_tag(
                slug=slug, name_pt=pt, name_en=en, name_es=es, env=env
            )
            tag_ids.append(r["id"])
            log(f"  created tag {slug} -> {r['id']}")
        else:
            raise SystemExit(
                f"tag {slug!r} neither exists nor is declared new"
            )

    # ── dataset: refresh description + tags, keep organizations/themes ──────
    # Prod is always published: this script only ever extends the existing,
    # already-public dataset, so selecting under_review there would hide the
    # live tables rather than stage a new one. On dev/staging the default stays
    # under_review and --publish opts in.
    status_id = (
        status["published"]
        if env == "prod" or args.publish
        else status["under_review"]
    )
    r = server.create_update_dataset(
        slug=DATASET_SLUG,
        name_pt=ds["name_pt"],
        name_en=ds["name_en"],
        name_es=ds["name_es"],
        description_pt=DATASET_DESCRIPTION["pt"],
        description_en=DATASET_DESCRIPTION["en"],
        description_es=DATASET_DESCRIPTION["es"],
        organization_ids=[o["id"] for o in ds["organizations"]],
        theme_ids=[t["id"] for t in ds["themes"]],
        tag_ids=tag_ids,
        status_id=status_id,
        id=dataset_id,
        env=env,
    )
    log(
        f"dataset updated: {r.get('id', r)} status={'published' if args.publish else 'under_review'}"
    )

    # ── raw data source ─────────────────────────────────────────────────────
    existing_rds = {
        s["url"]: s["id"]
        for s in server.get_raw_data_sources(DATASET_SLUG, env=env)
    }
    rds_id = existing_rds.get(RAW_SOURCE["url"])
    r = server.create_update_raw_data_source(
        dataset_id=dataset_id,
        name_pt=RAW_SOURCE["name_pt"],
        name_en=RAW_SOURCE["name_en"],
        name_es=RAW_SOURCE["name_es"],
        description_pt=RAW_SOURCE["description_pt"],
        description_en=RAW_SOURCE["description_en"],
        description_es=RAW_SOURCE["description_es"],
        url=RAW_SOURCE["url"],
        license_id=license_[RAW_SOURCE["license_slug"]],
        availability_id=avail[RAW_SOURCE["availability_slug"]],
        has_structured_data=True,
        is_free=True,
        contains_api=False,
        requires_registration=False,
        id=rds_id,
        env=env,
    )
    rds_id = r["id"]
    log(f"raw data source: {rds_id}")

    # ── phase 1: every create_update_table, BEFORE any coverage ─────────────
    ds = server.get_dataset(DATASET_SLUG, env=env)
    table_ids = {}
    for slug in want:
        m = TABLE_META[slug]
        existing = ds["tables"].get(slug)
        r = server.create_update_table(
            slug=slug,
            name_pt=m["name_pt"],
            name_en=m["name_en"],
            name_es=m["name_es"],
            description_pt=m["description_pt"],
            description_en=m["description_en"],
            description_es=m["description_es"],
            dataset_id=dataset_id,
            status_id=status["published"],
            published_by_ids=[account_id],
            data_cleaned_by_ids=[account_id],
            raw_data_source_ids=[rds_id],
            id=existing["id"] if existing else None,
            env=env,
        )
        table_ids[slug] = r["id"]
        log(f"table {slug}: {r['id']}")

    # ── phase 2: OLs, columns, cloud table, coverage, range, update ─────────
    ds = server.get_dataset(DATASET_SLUG, env=env)
    for slug in want:
        m = TABLE_META[slug]
        tid = table_ids[slug]
        live = ds["tables"][slug]
        log(f"=== {slug} ===")

        ol_by_entity = {
            o["entity_slug"]: o["id"]
            for o in live.get("observation_levels", [])
        }
        ol_ids = {}
        for ent_slug in m["observation_levels"]:
            r = server.create_update_observation_level(
                table_id=tid,
                entity_id=entity[ent_slug],
                id=ol_by_entity.get(ent_slug),
                env=env,
            )
            ol_ids[ent_slug] = r["id"]
        log(f"  observation levels: {ol_ids}")

        payload = (COLUMNS_JSON / f"{slug}.json").read_text()
        r = server.bulk_upsert_columns(
            table_id=tid, columns_json=payload, env=env
        )
        log(
            f"  columns: {r.get('created', '?')} created, {r.get('updated', '?')} updated"
        )

        live2 = server.get_dataset(DATASET_SLUG, env=env)["tables"][slug]
        col_ids = {c["name"]: c["id"] for c in live2["columns"]}
        col_to_ol = {
            col: ol_ids[ent]
            for ent, cols in m["observation_levels"].items()
            for col in cols
        }
        parts = partition_columns(slug)
        for name in sorted(set(col_to_ol) | parts):
            if name not in col_ids:
                raise SystemExit(f"{slug}: column {name!r} not registered")
            server.update_column(
                column_id=col_ids[name],
                column_name=name,
                table_id=tid,
                observation_level_id=col_to_ol.get(name),
                is_partition=name in parts,
                is_primary_key=False,
                env=env,
            )
        log(
            f"  linked OL on {sorted(col_to_ol)}, partition on {sorted(parts)}"
        )

        ct = live.get("cloud_tables") or []
        r = server.create_update_cloud_table(
            table_id=tid,
            gcp_project_id=GCP_PROJECT[env],
            gcp_dataset_id=GCP_DATASET,
            gcp_table_id=slug,
            id=ct[0]["id"] if ct else None,
            env=env,
        )
        log(f"  cloud table: {GCP_PROJECT[env]}.{GCP_DATASET}.{slug}")

        cov = live.get("coverages") or []
        r = server.create_update_coverage(
            table_id=tid,
            area_id=area_id,
            id=cov[0]["id"] if cov else None,
            env=env,
        )
        cov_id = r["id"]
        sy, sm, ey, em = m["coverage"]
        existing_dr = (cov[0].get("datetime_ranges") if cov else None) or []
        server.create_update_datetime_range(
            coverage_id=cov_id,
            start_year=sy,
            start_month=sm,
            end_year=ey,
            end_month=em,
            interval=1,
            is_closed=False,
            id=existing_dr[0]["id"] if existing_dr else None,
            env=env,
        )
        log(f"  coverage {AREA_SLUG}: {sy}-{sm:02d} .. {ey}-{em:02d}")

        upd = live.get("updates") or []
        # `latest` is a DateTime on the backend, not a Date, and for a
        # table-anchored Update it means when Data Basis last refreshed the
        # table — today — never the max date in the data.
        latest = f"{date.today().isoformat()}T00:00:00"
        server.create_update_update(
            entity_id=entity[m["update_entity"]],
            frequency=1,
            latest=latest,
            table_id=tid,
            id=upd[0]["id"] if upd else None,
            env=env,
        )
        log(f"  update: entity={m['update_entity']} latest={latest}")

    # ── phase 3: table order ────────────────────────────────────────────────
    if len(want) == len(TABLE_META):
        server.reorder_tables(
            dataset_slug=DATASET_SLUG, table_slugs=TABLE_ORDER, env=env
        )
        log(f"reordered {len(TABLE_ORDER)} tables")

    log("DONE")


if __name__ == "__main__":
    main()
