"""One-shot bootstrap: load the current OpenAlex snapshot into basedosdados-dev staging.

    uv run python -m models.world_openalex.code.load --fresh            # first run
    uv run python -m models.world_openalex.code.load                    # resume
    uv run python -m models.world_openalex.code.load --fresh --entities topics fields --max-files 1

Calls the same ``pipelines.datasets.world_openalex.loader.load_snapshot`` the
recurring flow uses, against the dev bucket. Resume state lives in GCS as
per-file markers (see loader.py); scratch for in-flight files defaults to ``~/Library/Caches/world_openalex_data`` — local and
unsynced — and can be moved with ``WORLD_OPENALEX_SCRATCH``.

Requires GOOGLE_APPLICATION_CREDENTIALS pointing at the basedosdados-dev
service-account key.
"""

import argparse
import os
from pathlib import Path

from pipelines.datasets.world_openalex.constants import constants
from pipelines.datasets.world_openalex.loader import load_snapshot

SCRATCH = Path(
    os.environ.get(
        "WORLD_OPENALEX_SCRATCH",
        Path.home() / "Library/Caches/world_openalex_data",
    )
)


def main() -> None:
    """Parse arguments and run the load."""
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument(
        "--fresh",
        action="store_true",
        help="reload everything even if resumable",
    )
    ap.add_argument(
        "--entities", nargs="+", choices=list(constants.ENTITY_TABLES.value)
    )
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument(
        "--max-files", type=int, help="files per entity, for tests"
    )
    args = ap.parse_args()
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        raise SystemExit("GOOGLE_APPLICATION_CREDENTIALS is not set")
    result = load_snapshot(
        bucket_name="basedosdados-dev",
        scratch=SCRATCH,
        entities=args.entities,
        workers=args.workers,
        fresh=args.fresh,
        max_files=args.max_files,
    )
    print(f"\nRelease {result['release']}")
    for entity, (got, expected) in result["checks"].items():
        print(f"  {entity:13} {got:>13,} / {expected:>13,} records")
    for table, n in sorted(result["rows"].items()):
        print(f"  {table:32} {n:>15,} rows")


if __name__ == "__main__":
    main()
