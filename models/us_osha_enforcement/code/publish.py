#!/usr/bin/env python
"""Flip the dataset's status between ``under_review`` and ``published``.

    python models/us_osha_enforcement/code/publish.py --env staging --status published

On dev/staging this is safe at any point and is done before the PR, so a
reviewer sees the dataset as it will appear. On **prod** it is a separate
post-merge action: only once the PR is merged, the table-approve action has
materialised `basedosdados.us_osha_enforcement.*`, and the live tables and
metadata are verified.

The API does no partial updates, so every required field is re-passed.
"""

from __future__ import annotations

import argparse
import importlib.util
import logging
import sys
from pathlib import Path

MCP = "/Users/rdahis/Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp"
HERE = Path(__file__).resolve().parent
SLUG = "enforcement"

log = logging.getLogger("publish")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--env", default="staging")
    p.add_argument(
        "--status", default="published", choices=["published", "under_review"]
    )
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    sys.path.insert(0, MCP)
    import server

    spec = importlib.util.spec_from_file_location(
        "rm", HERE / "register_metadata.py"
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load register_metadata")
    rm = importlib.util.module_from_spec(spec)
    sys.modules["rm"] = rm
    spec.loader.exec_module(rm)

    env = args.env
    ids = server.discover_ids(env=env, keys=["status", "theme", "tag"])
    existing = server.get_dataset(slug=SLUG, env=env)
    if not existing.get("found"):
        log.error(f"dataset {SLUG} not found on {env}")
        return 1
    org_id = existing["organizations"][0]["id"]
    known = set(ids["tag"].values())
    tag_ids = [t for t in rm.TAGS if t in known]
    theme_ids = [ids["theme"][t] for t in ("safety", "economics", "justice")]
    server.create_update_dataset(
        id=existing["id"],
        slug=SLUG,
        organization_ids=[org_id],
        theme_ids=theme_ids,
        tag_ids=tag_ids,
        status_id=ids["status"][args.status],
        env=env,
        **rm.DATASET_TEXT,
    )
    log.info(f"{SLUG} on {env}: status -> {args.status} ({len(tag_ids)} tags)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
