"""One-shot historical backfill for the br_mgi_pncp onboarding.

Front end for ``pipelines.datasets.br_mgi_pncp.utils.harvest``, which is the
canonical transform shared with the recurring pipeline.

The backfill harvests ``contratacao`` and ``contrato`` from their
*publication-date* endpoints rather than the update-date endpoints the pipeline
uses: it wants each record filed under the year it was published, and the
publication endpoints page more predictably over a fixed history. Every other
table has no publication-date endpoint and uses the same path either way.

PASS AN EXPLICIT ``--end`` FOR A MULTI-DAY BACKFILL. ``--end`` defaults to
today, and a chunk's filename is its window, so when the date rolls over the
final partial window is renamed and re-fetched: wasted work, an orphaned
chunk, and a spurious failure report. Pinning the cutoff keeps the window set
stable across restarts. Anything after the cutoff is picked up by the
recurring pipeline's lookback, which is what the handover is for.

Usage:
    uv run python models/br_mgi_pncp/code/download.py --end 2026-08-28 [--tables ...]
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.br_mgi_pncp.constants import constants
from pipelines.datasets.br_mgi_pncp.utils import harvest

DATA_DIR = Path(
    os.environ.get(
        "PNCP_DATA_DIR", Path.home() / "Downloads" / "br_mgi_pncp_data"
    )
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
