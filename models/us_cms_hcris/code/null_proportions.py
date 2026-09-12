"""Measure each column's non-null share, to build the dbt test exemption lists.

    python null_proportions.py

``not_null_proportion_multiple_columns`` asserts every column is at least 5%
non-null, and a column that is legitimately sparser has to be exempted by name.
The exemptions are measured, never guessed:

* ``report`` and ``report_value`` are read from the **parquet footers** — every
  column chunk records its null count, so this is metadata only: no BigQuery
  scan and no quota spend. See [[reference_null_proportion_test_cost]].
* ``hospital_financial`` does not exist as parquet; it is a dbt model. Its
  shares come from ``measured.json``, which ``verify_measures.py`` wrote from
  the same parquet.

Exempting a column only withdraws an assertion about it, so anything close to
the threshold is exempted rather than left to drift.
"""

import json
from pathlib import Path

import pyarrow.parquet as pq
from common import OUTPUT
from measures import MEASURES

CODE_DIR = Path(__file__).resolve().parent
THRESHOLD = 0.05
# A column between the threshold and this is exempted too: its share moves as
# CMS publishes more of the current fiscal year, and an exemption is the safe
# direction to err.
MARGIN = 0.10


def parquet_shares(table: str) -> dict[str, float]:
    """Non-null share per column, read from the parquet footers.

    Args:
        table: Table slug under ``OUTPUT``.

    Returns:
        Column name to non-null share.
    """
    rows = 0
    nulls: dict[str, int] = {}
    for file in sorted((OUTPUT / table).rglob("*.parquet")):
        md = pq.ParquetFile(file).metadata
        rows += md.num_rows
        for g in range(md.num_row_groups):
            group = md.row_group(g)
            for c in range(group.num_columns):
                col = group.column(c)
                name = col.path_in_schema
                nulls[name] = nulls.get(name, 0) + col.statistics.null_count
    return {k: (rows - v) / rows for k, v in nulls.items()} if rows else {}


def measure_shares() -> dict[str, float]:
    """Non-null share per curated measure, across both form versions.

    Returns:
        Measure name to share of all published reports.
    """
    measured = json.loads((CODE_DIR / "measured.json").read_text())
    reports = sum(
        pq.ParquetFile(f).metadata.num_rows
        for f in (OUTPUT / "report").rglob("*.parquet")
    )
    out = {}
    for m in MEASURES:
        hit = sum(v["reports"] for v in measured.get(m.name, {}).values())
        out[m.name] = hit / reports
    return out


def report(name: str, shares: dict[str, float]) -> list[str]:
    """Print a table's shares and return the columns to exempt.

    Args:
        name: Table slug.
        shares: Column to non-null share.

    Returns:
        Sorted names of the columns below the threshold plus margin.
    """
    print(f"\n=== {name} ===")
    exempt = []
    for col, share in sorted(shares.items(), key=lambda kv: kv[1]):
        flag = ""
        if share < THRESHOLD:
            flag = "  EXEMPT (below 5%)"
        elif share < THRESHOLD + MARGIN:
            flag = "  EXEMPT (within margin)"
        if flag:
            exempt.append(col)
        print(f"  {col:<44} {share:7.2%}{flag}")
    return sorted(exempt)


def main() -> None:
    """Print every table's shares and the exemption lists to paste into schema.py."""
    out = {}
    for table in ("report", "report_value"):
        out[table] = report(table, parquet_shares(table))
    out["hospital_financial"] = report("hospital_financial", measure_shares())
    print("\n\nIGNORE = " + json.dumps(out, indent=4))
    (CODE_DIR / "null_proportions.json").write_text(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
