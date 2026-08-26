"""One-shot historical backfill for the br_pncp onboarding.

Front end for ``pipelines.datasets.br_pncp.utils.harvest``, which is the
canonical transform shared with the recurring pipeline.

The backfill harvests ``contratacao`` and ``contrato`` from their
*publication-date* endpoints rather than the update-date endpoints the pipeline
uses: it wants each record filed under the year it was published, and the
publication endpoints page more predictably over a fixed history. Every other
table has no publication-date endpoint and uses the same path either way.

Usage:
    uv run python models/br_pncp/code/download.py [--tables ...] [--start ...]
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.br_pncp.constants import constants
from pipelines.datasets.br_pncp.utils import harvest

DATA_DIR = Path(
    os.environ.get("PNCP_DATA_DIR", Path.home() / "Downloads" / "br_pncp_data")
)
TABLES = list(constants.ENDPOINTS.value)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tables", nargs="*", default=TABLES, choices=TABLES)
    ap.add_argument(
        "--start",
        type=date.fromisoformat,
        default=date(constants.START_YEAR.value, 1, 1),
    )
    ap.add_argument("--end", type=date.fromisoformat, default=date.today())
    ap.add_argument("--input-dir", type=Path, default=DATA_DIR / "input")
    args = ap.parse_args()

    backfill_paths = constants.BACKFILL_PATHS.value
    for table in args.tables:
        print(f"### {table} {args.start} .. {args.end}", flush=True)
        total = harvest(
            table=table,
            input_dir=args.input_dir,
            start=args.start,
            end=args.end,
            path_override=backfill_paths.get(table),
        )
        print(f"== {table}: {total:,} new records this run", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
