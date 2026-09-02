"""Reconcile the staged contribution table against the backfill and the codebook.

Three numbers have to agree for every cycle:

1. what ``run_backfill`` counted in the Parquet parts before uploading them,
2. what BigQuery actually holds after the wildcard load, and
3. the codebook's published figure, which is a **ceiling** rather than an
   equality — it counts physical lines, so it exceeds the record count wherever
   a field contains a newline inside quotes.

(1) against (2) is the check that matters for the streaming upload: parts are
written, shipped and deleted one at a time while the conversion is still
running, so a part silently lost or shipped twice is exactly the failure this
design could produce. A per-cycle count is the only thing that would show it.

Costs one scan of a single column.

    export GOOGLE_APPLICATION_CREDENTIALS=<dev service account key>
    uv run --with google-cloud-bigquery python reconcile.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from google.cloud import bigquery

sys.path.insert(0, str(Path(__file__).resolve().parent))
import clean
import constants

PROJECT = "basedosdados-dev"
STATE = Path(__file__).resolve().parent / "backfill_state.json"


def main() -> None:
    state = json.loads(STATE.read_text())["cycles"]
    client = bigquery.Client(project=PROJECT)
    job = client.query(
        f"select cycle, count(*) n "
        f"from `{PROJECT}.us_stanford_dime_staging.contribution` "
        f"group by cycle order by cycle"
    )
    in_bq = {r.cycle: r.n for r in job.result()}
    print(f"billed {(job.total_bytes_billed or 0) / 1e9:.2f} GB\n")

    header = f"{'cycle':>6} {'backfill':>14} {'BigQuery':>14} {'ceiling':>14}  status"
    print(header)
    ok = True
    for cyc in sorted(in_bq, key=int):
        local = state.get(cyc, {}).get("rows")
        ceiling = constants.CODEBOOK_ROWS[int(cyc)] - 1
        if local is None:
            status, ok = "NOT IN BACKFILL STATE", False
        elif local != in_bq[cyc]:
            status, ok = f"MISMATCH ({in_bq[cyc] - local:+,})", False
        elif in_bq[cyc] > ceiling:
            status, ok = "ABOVE CODEBOOK CEILING", False
        else:
            status = "ok"
        print(
            f"{cyc:>6} {local or 0:>14,} {in_bq[cyc]:>14,} {ceiling:>14,}  {status}"
        )

    missing = [c for c in clean.CYCLES if str(c) not in in_bq]
    total = sum(in_bq.values())
    print(f"\ncycles present : {len(in_bq)}/{len(clean.CYCLES)}")
    if missing:
        print(f"cycles missing : {missing}")
        ok = False
    print(f"rows in BigQuery: {total:,}")
    if not missing:
        ceiling_total = sum(constants.CODEBOOK_ROWS.values()) - len(
            clean.CYCLES
        )
        print(
            f"codebook ceiling: {ceiling_total:,}  "
            f"({ceiling_total - total:,} lines are newlines inside quoted fields)"
        )
    print("\nRESULT:", "reconciled" if ok else "DISCREPANCY — do not promote")
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
