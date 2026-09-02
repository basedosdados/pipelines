"""Assemble the pre-promotion verification checklist from live state.

Reads what is actually in BigQuery and the backend rather than what the run
logs claimed, so the checklist presented for approval reflects the deployed
state and not the intent.

    export GOOGLE_APPLICATION_CREDENTIALS=<dev service account key>
    uv run --with google-cloud-bigquery --with requests python checkpoint.py
"""

from __future__ import annotations

import sys
from pathlib import Path

from google.cloud import bigquery

sys.path.insert(0, str(Path(__file__).resolve().parent))
import architecture as arch
import constants

PROJECT = "basedosdados-dev"
DATASET = "us_stanford_dime"
SLUG = "dime"
TABLES = list(arch.TABLES)


def table_facts(client: bigquery.Client) -> dict:
    """Row counts and partitioning for the built tables, from table metadata.

    Uses ``get_table`` rather than ``count(*)``: metadata is free, and a count
    over an 861M-row table is not.
    """
    out = {}
    for t in TABLES:
        try:
            tb = client.get_table(f"{PROJECT}.{DATASET}.{t}")
            out[t] = {
                "rows": tb.num_rows,
                "gb": (tb.num_bytes or 0) / 1e9,
                "partition": tb.range_partitioning.field
                if tb.range_partitioning
                else None,
                "columns": len(tb.schema),
            }
        except Exception as exc:
            out[t] = {"error": type(exc).__name__}
    return out


def staging_facts(client: bigquery.Client) -> dict:
    out = {}
    for t in TABLES:
        try:
            tb = client.get_table(f"{PROJECT}.{DATASET}_staging.{t}")
            out[t] = tb.num_rows
        except Exception:
            out[t] = None
    return out


def main() -> None:
    client = bigquery.Client(project=PROJECT)
    built = table_facts(client)
    staged = staging_facts(client)

    print("=== VERIFICATION CHECKPOINT — us_stanford_dime (dev/staging) ===\n")
    print(f"Dataset slug : {SLUG}")
    print(f"GCP dataset  : {PROJECT}.{DATASET}")
    print(f"Licence      : {constants.LICENSE}")
    print(f"Source       : {constants.LANDING_PAGE}\n")

    print(
        f"{'table':<20} {'built rows':>15} {'staging rows':>15} {'cols':>5} "
        f"{'size':>9}  partition"
    )
    all_ok = True
    for t in TABLES:
        b = built[t]
        if "error" in b:
            print(
                f"{t:<20} {'NOT BUILT':>15} {staged[t] or 0:>15,} "
                f"{'-':>5} {'-':>9}  -"
            )
            all_ok = False
            continue
        match = "" if staged[t] == b["rows"] else "  <-- STAGING MISMATCH"
        if match:
            all_ok = False
        print(
            f"{t:<20} {b['rows']:>15,} {staged[t] or 0:>15,} "
            f"{b['columns']:>5} {b['gb']:>8.1f}G  {b['partition'] or '-'}{match}"
        )

    total = sum(b.get("rows", 0) for b in built.values())
    print(f"\ntotal rows built: {total:,}")

    expected = sum(constants.CODEBOOK_ROWS.values()) - len(
        constants.CODEBOOK_ROWS
    )
    c = built.get("contribution", {}).get("rows")
    if c:
        print(
            f"contribution vs codebook ceiling: {c:,} of {expected:,} "
            f"({expected - c:,} lines are newlines inside quoted fields)"
        )

    print("\nChecks to run before approving:")
    print(
        "  reconcile.py                       per-cycle rows vs backfill and codebook"
    )
    print(
        "  verify_cast.py contribution --cycle 2024   no column emptied by a cast"
    )
    print("  dbt test --select models/us_stanford_dime  all model tests")
    print(f"\nVerify at: https://staging.basedosdados.org/dataset/{SLUG}")
    print(
        "\nRESULT:",
        "ready to present" if all_ok else "NOT READY — see rows above",
    )


if __name__ == "__main__":
    main()
