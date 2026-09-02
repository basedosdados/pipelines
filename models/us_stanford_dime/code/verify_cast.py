"""Compare non-null counts between the staging and built tables.

``verify_clean.py`` does this against the raw CSV, but the backfill deletes each
cycle's source as soon as it lands, so it cannot be run after the fact. This is
the post-load equivalent, and it catches the same silent failure: ``safe_cast``
returns NULL rather than raising, so a format it does not accept empties a
column while the row count is unchanged and every dbt test still passes.

The staging side is **read from ``sparsity.json``**, not re-queried. The staging
tables are created by ``load_table_from_uri``, which does not partition them,
and ``cycle`` is a STRING there — so a cycle-scoped query against staging prunes
nothing and scans all 870M rows across every column. That scan costs ~345 GB and
``measure_sparsity`` has already paid it once, recording the per-column counts.
The built table *is* partitioned on an INT64 cycle, so its side prunes properly
and costs a fraction of that.

Run ``measure_sparsity.py`` first; this asserts the two agree on scope.

    uv run --with google-cloud-bigquery python verify_cast.py contribution
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from google.cloud import bigquery

sys.path.insert(0, str(Path(__file__).resolve().parent))
import architecture as arch
import gen_dbt

PROJECT = "basedosdados-dev"
DATASET = "us_stanford_dime"
SPARSITY = Path(__file__).resolve().parent / "sparsity.json"


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("table", choices=list(arch.TABLES))
    args = p.parse_args()
    table = args.table

    if not SPARSITY.exists():
        raise SystemExit(
            "sparsity.json missing — run measure_sparsity.py first"
        )
    measured = json.loads(SPARSITY.read_text()).get(table)
    if not measured:
        raise SystemExit(
            f"no staging measurement for {table} — run measure_sparsity.py"
        )

    # Mirror the scope measure_sparsity used, so the two sides are comparable.
    where = ""
    if table == "contribution":
        where = f"where cycle = {gen_dbt.SPARSITY_CYCLE}"

    cols = arch.column_names(table)
    sel = ", ".join(f"countif({c} is not null) as {c}" for c in cols)
    client = bigquery.Client(project=PROJECT)
    job = client.query(
        f"select count(*) as n_rows, {sel} from `{PROJECT}.{DATASET}.{table}` {where}"
    )
    row = dict(next(iter(job.result())).items())
    built_n = row.pop("n_rows")

    raw = measured["non_null"]
    raw_n = measured["rows"]
    print(f"{table}  (staging scope: {measured['where']})")
    print(
        f"  rows   staging {raw_n:,}   built {built_n:,}   "
        f"{'MATCH' if raw_n == built_n else 'MISMATCH'}"
    )
    print(
        f"  staging side reused from sparsity.json ({measured['gb_billed']} GB "
        f"already billed); built side billed {(job.total_bytes_billed or 0) / 1e9:.2f} GB\n"
    )

    print(f"{'column':<38} {'type':<8} {'staging':>14} {'built':>14}  status")
    ok = raw_n == built_n
    for col in arch.TABLES[table]:
        name, bq_type = col[0], col[1]
        r, b = raw.get(name, 0), row.get(name, 0)
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
