"""One-shot onboarding clean for us_fec_campaign_finance.

    python clean.py                      # every table, cycles 1980-2026
    python clean.py candidate committee  # selected tables only

Downloads each cycle's ZIP, parses it, writes all-STRING partitioned parquet under
$FEC_DATA_DIR/output, and deletes the ZIP immediately — peak disk stays near one
archive instead of the full 22 GB.

The transform itself lives in fec.py, which the recurring Prefect pipeline imports
too, so there is exactly one implementation of it.
"""

import sys
import time
from pathlib import Path

# The cleaning transform lives in the pipeline package so the recurring flow and
# this one-shot onboarding script share exactly one implementation
# (.claude/rules/prefect-pipeline-conventions.md, "DRY with the onboarding code").
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from pipelines.datasets.us_fec_campaign_finance import (
    utils as fec,
)

# Smallest first: the dimension tables land in minutes and unblock dbt and metadata
# work, while contribution_individual (~20 GB compressed) runs for hours.
ORDER = [
    "candidate",
    "committee",
    "candidate_committee_link",
    "contribution_committee",
    "disbursement",
    "committee_transaction",
    "contribution_individual",
]


def main(argv: list[str]) -> None:
    tables = argv or ORDER
    unknown = set(tables) - set(fec.SPECS)
    if unknown:
        raise SystemExit(f"unknown table(s): {sorted(unknown)}")

    started = time.time()
    totals = {}
    for table in tables:
        t0 = time.time()
        totals.update(fec.clean_all([table]))
        print(f"  ({time.time() - t0:.0f}s)", flush=True)

    print("\n=== clean complete ===")
    for table, rows in totals.items():
        print(f"{table:26s} {rows:>14,}")
    print(
        f"total rows {sum(totals.values()):,} in {(time.time() - started) / 60:.1f} min"
    )


if __name__ == "__main__":
    main(sys.argv[1:])
