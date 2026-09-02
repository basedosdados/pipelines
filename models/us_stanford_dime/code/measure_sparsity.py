"""Measure per-column non-null proportion on the STAGING tables.

``gen_dbt.py`` uses the result to exempt legitimately sparse columns from
``not_null_proportion_multiple_columns``.

Measuring on staging rather than on the built table is the whole point. Staging
is the raw all-STRING Parquet, so it is independent of the casts the test is
meant to police; deriving the exemptions from the built table instead would let
a column emptied by a bad cast look legitimately sparse and be excused by the
very test that should have caught it.

Cost control: the contribution table is 861M rows, so it is measured on a
single partition. One pass computes every column's count, and the query reports
the bytes it billed.

    export GOOGLE_APPLICATION_CREDENTIALS=<dev service account key>
    uv run --with google-cloud-bigquery python measure_sparsity.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from google.cloud import bigquery

sys.path.insert(0, str(Path(__file__).resolve().parent))
import architecture as arch
import gen_dbt

PROJECT = "basedosdados-dev"
STAGING = f"{PROJECT}.us_stanford_dime_staging"
OUT = Path(__file__).resolve().parent / "sparsity.json"

# Below this share of non-null rows a column is exempted from the test.
THRESHOLD = 0.05

# Partition filter per table, so a huge table is not scanned whole.
WHERE = {"contribution": f"where cycle = '{gen_dbt.TEST_CYCLE}'"}


def measure(table: str, client: bigquery.Client) -> dict:
    cols = arch.column_names(table)
    where = WHERE.get(table, "")
    # Staging is all-STRING, so an empty string counts as absent too.
    sel = ", ".join(
        f"countif({c} is not null and {c} != '') as {c}" for c in cols
    )
    sql = f"select count(*) as n_rows, {sel} from `{STAGING}.{table}` {where}"
    job = client.query(sql)
    row = dict(next(iter(job.result())).items())
    n = row.pop("n_rows")
    gb = (job.total_bytes_billed or 0) / 1e9
    sparse = sorted(c for c, v in row.items() if n and v / n < THRESHOLD)
    print(
        f"{table}: {n:,} rows scanned, {gb:.2f} GB billed, "
        f"{len(sparse)} column(s) below {THRESHOLD:.0%}"
    )
    for c in sparse:
        print(f"    {c:<38} {100 * row[c] / n:6.3f}% non-null")
    return {
        "rows": n,
        "where": where or "(whole table)",
        "gb_billed": round(gb, 3),
        "sparse": sparse,
        "non_null": {c: row[c] for c in cols},
    }


def main() -> None:
    tables = sys.argv[1:] or list(arch.TABLES)
    client = bigquery.Client(project=PROJECT)
    out = json.loads(OUT.read_text()) if OUT.exists() else {}
    for t in tables:
        out[t] = measure(t, client)
        OUT.write_text(json.dumps(out, indent=2, sort_keys=True))
    print(f"\nwrote {OUT.name}; re-run gen_dbt.py to fold the exemptions in")


if __name__ == "__main__":
    main()
