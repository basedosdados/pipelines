"""Flip the us_census_bps dataset status.

Data Basis publishes the dev/staging dataset before promotion so a reviewer
sees it as it will appear, and publishes the production dataset only after the
onboarding PR is merged, the table-approve action has materialised the prod
tables, and those tables are verified.

CreateUpdateDataset replaces rather than patches, so every required field is
re-sent from the same definitions the registration used.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(
    0, "/Users/rdahis/Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp"
)
sys.path.insert(0, str(Path(__file__).resolve().parent))

import server
from metadata import (
    DATASET_SLUG,
    DESC_EN,
    DESC_ES,
    DESC_PT,
    NAME_EN,
    NAME_ES,
    NAME_PT,
    NEW_TAGS,
    TAG_SLUGS,
    THEME_SLUGS,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="staging")
    parser.add_argument(
        "--status", default="published", choices=["published", "under_review"]
    )
    args = parser.parse_args()
    env = args.env

    ids = server.discover_ids(env=env, keys=["status", "theme"])
    org = server.lookup_id("organization", "census_bureau", env=env)["id"]
    tag_ids = [
        server.lookup_id("tag", slug, env=env)["id"]
        for slug in TAG_SLUGS[env] + [t["slug"] for t in NEW_TAGS]
    ]
    dataset = server.get_dataset(DATASET_SLUG, env=env)
    if not dataset.get("found"):
        raise SystemExit(f"dataset {DATASET_SLUG} not found in {env}")

    server.create_update_dataset(
        slug=DATASET_SLUG,
        name_pt=NAME_PT,
        name_en=NAME_EN,
        name_es=NAME_ES,
        description_pt=DESC_PT,
        description_en=DESC_EN,
        description_es=DESC_ES,
        organization_ids=[org],
        theme_ids=[ids["theme"][t] for t in THEME_SLUGS],
        tag_ids=tag_ids,
        status_id=ids["status"][args.status],
        id=dataset["id"],
        env=env,
    )
    after = server.get_dataset(DATASET_SLUG, env=env)
    print(f"{DATASET_SLUG} in {env}: status now {after.get('status')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
