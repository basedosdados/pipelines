"""Compare non-null counts between the staging and built tables in BigQuery.

``verify_clean.py`` does the same job against the raw CSV, but it needs the
source file, and the contribution backfill deletes each cycle's source as soon
as it lands — there is no room to keep 75 GiB around. This is the post-load
equivalent: staging is the raw all-STRING Parquet, the built table is the
``safe_cast`` output, and any column where built < staging is a cast the data
outgrew.

That failure is silent by construction. ``safe_cast`` returns NULL rather than
raising, so a format it does not accept empties the column while the row count
is unchanged and every dbt test still passes. On another dataset this destroyed
a date column across 97M rows with all tests green.

Cost control: the contribution table is 861M rows, so it is compared on a single
cycle. Both sides are scanned once and the bytes billed are reported.

    export GOOGLE_APPLICATION_CREDENTIALS=<dev service account key>
    uv run --with google-cloud-bigquery python verify_cast.py contribution --cycle 2024
    uv run --with google-cloud-bigquery python verify_cast.py recipient
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from google.cloud import bigquery

sys.path.insert(0, str(Path(__file__).resolve().parent))
import architecture as arch

PROJECT = "basedosdados-dev"
DATASET = "us_stanford_dime"


def counts(
    client: bigquery.Client, table: str, staging: bool, where: str
) -> tuple[dict, int, float]:
    ref = (
        f"{PROJECT}.{DATASET}_staging.{table}"
        if staging
        else f"{PROJECT}.{DATASET}.{table}"
    )
    cols = arch.column_names(table)
    if staging:
        # Staging is all-STRING; an empty string is absent too.
        sel = ", ".join(
            f"countif({c} is not null and {c} != '') as {c}" for c in cols
        )
    else:
        sel = ", ".join(f"countif({c} is not null) as {c}" for c in cols)
    job = client.query(
        f"select count(*) as n_rows, {sel} from `{ref}` {where}"
    )
    row = dict(next(iter(job.result())).items())
    n = row.pop("n_rows")
    return row, n, (job.total_bytes_billed or 0) / 1e9


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("table", choices=list(arch.TABLES))
    p.add_argument(
        "--cycle", type=int, help="restrict both sides to one cycle"
    )
    args = p.parse_args()

    where = f"where cycle = {args.cycle}" if args.cycle else ""
    staging_where = f"where cycle = '{args.cycle}'" if args.cycle else ""
    client = bigquery.Client(project=PROJECT)

    raw, raw_n, raw_gb = counts(client, args.table, True, staging_where)
    built, built_n, built_gb = counts(client, args.table, False, where)

    scope = f"cycle {args.cycle}" if args.cycle else "whole table"
    print(f"{args.table} ({scope})")
    print(
        f"  rows   staging {raw_n:,}   built {built_n:,}   "
        f"{'MATCH' if raw_n == built_n else 'MISMATCH'}"
    )
    print(f"  billed staging {raw_gb:.2f} GB + built {built_gb:.2f} GB\n")

    print(f"{'column':<38} {'type':<8} {'staging':>14} {'built':>14}  status")
    ok = raw_n == built_n
    for col in arch.TABLES[args.table]:
        name, bq_type = col[0], col[1]
        r, b = raw.get(name, 0), built.get(name, 0)
        if b < r:
            status = f"CAST LOSS {r - b:,}"
            ok = False
        elif r == 0:
            status = "empty in source"
        else:
            status = "ok"
        print(f"{name:<38} {bq_type:<8} {r:>14,} {b:>14,}  {status}")

    print(
        "\nRESULT:",
        "OK — every column survived its cast" if ok else "CAST LOSS DETECTED",
    )
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
