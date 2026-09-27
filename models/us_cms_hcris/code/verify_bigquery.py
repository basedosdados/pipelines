"""Check the built us_cms_hcris models against the local parquet and measured.json.

    python verify_bigquery.py                # dev (basedosdados-dev)
    python verify_bigquery.py --project basedosdados

The dbt tests assert structure — keys, nulls, foreign keys, dictionary coverage.
This asserts that what landed is the data that was cleaned: the row counts match
the parquet footers, the partitions span the years the parquet does, the 107
report ids CMS reused across form versions survive as two distinct hospitals
rather than one merged row, and each curated measure is populated for the same
number of reports ``verify_measures.py`` counted locally.

Every query is scoped or aggregate; none scans a whole table without need.
"""

import argparse
import json
import sys
from pathlib import Path

import pyarrow.parquet as pq
from common import DATASET_ID, OUTPUT, STAGED_TABLES
from google.cloud import bigquery
from measures import MEASURES

CODE_DIR = Path(__file__).resolve().parent
TOLERANCE = 0.001


def local_rows(table: str) -> int:
    """Row count of a table's local parquet, from the footers.

    Args:
        table: Table slug.

    Returns:
        Total rows.
    """
    return sum(
        pq.ParquetFile(f).metadata.num_rows
        for f in (OUTPUT / table).rglob("*.parquet")
    )


def check(label: str, got, want, ok: bool | None = None) -> bool:
    """Print one comparison and return whether it passed.

    Args:
        label: What is being compared.
        got: Observed value.
        want: Expected value.
        ok: Override the equality test.

    Returns:
        True when the check passed.
    """
    passed = (got == want) if ok is None else ok
    mark = "ok  " if passed else "FAIL"
    print(f"  {mark} {label}: got {got}, expected {want}")
    return passed


def main() -> None:
    """Run every check and exit non-zero on the first failure."""
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--project", default="basedosdados-dev")
    args = ap.parse_args()
    client = bigquery.Client(project=args.project)
    ds = f"{args.project}.{DATASET_ID}"

    def one(sql: str):
        return next(iter(client.query(sql).result()))

    failures = []

    print("row counts against the cleaned parquet")
    for table in STAGED_TABLES:
        got = one(f"select count(*) n from `{ds}.{table}`").n
        failures.append(not check(table, f"{got:,}", f"{local_rows(table):,}"))
    got = one(f"select count(*) n from `{ds}.hospital_financial`").n
    failures.append(
        not check(
            "hospital_financial", f"{got:,}", f"{local_rows('report'):,}"
        )
    )

    print("\npartition span")
    for table in ("report", "report_value", "hospital_financial"):
        r = one(
            f"select min(year) a, max(year) b, count(distinct year) n from `{ds}.{table}`"
        )
        failures.append(
            not check(
                table, f"{r.a}-{r.b} ({r.n} years)", "1996-2026 (31 years)"
            )
        )

    print("\nthe 107 report ids CMS reused across form versions")
    r = one(f"""
        with reused as (
            select report_id
            from `{ds}.hospital_financial`
            group by report_id having count(*) > 1)
        select
            count(*) rows_kept,
            countif(hospitals = 1) merged
        from (
            select h.report_id, count(distinct h.provider_ccn) hospitals, count(*) n
            from `{ds}.hospital_financial` h
            join reused using (report_id)
            group by h.report_id)
    """)
    # 107 reused ids, each surviving as its own row per form version. A merge on
    # report_id alone would leave 107 rows, each holding two hospitals' values.
    failures.append(not check("reused ids kept apart", r.rows_kept, 107))
    failures.append(not check("ids collapsed to one hospital", r.merged, 0))

    print("\ncurated measures against the locally measured coverage")
    measured = json.loads((CODE_DIR / "measured.json").read_text())
    cols = ", ".join(
        f"countif({m.name} is not null) as {m.name}" for m in MEASURES
    )
    row = one(f"select {cols} from `{ds}.hospital_financial`")
    off = 0
    for m in MEASURES:
        want = sum(v["reports"] for v in measured[m.name].values())
        got = getattr(row, m.name)
        # An exact match is expected; the tolerance only absorbs the few rows
        # where a measure sums to NULL because every mapped cell was NULL.
        if abs(got - want) > max(1, TOLERANCE * want):
            off += 1
            check(m.name, f"{got:,}", f"{want:,}", ok=False)
    failures.append(off > 0)
    if not off:
        print(
            f"  ok   all {len(MEASURES)} measures within {TOLERANCE:.1%} of the "
            "count measured locally"
        )

    if any(failures):
        sys.exit(f"\n{sum(failures)} check(s) failed")
    print("\nall checks passed")


if __name__ == "__main__":
    main()
