"""Measure each column's non-null share, to build the dbt test's ignore list.

not_null_proportion_multiple_columns asserts every column is at least 5%
populated. That is the wrong expectation for a table built as the union of 87
questionnaire versions: a column introduced in 2023 Q4 is absent from the first
77 waves by construction, and a follow-up question asked only of, say, domestic
workers is legitimately sparse everywhere.

This pass measures the real non-null share per column over the whole table and
over the twelve most recent waves, and writes the columns below the threshold to
sparse_columns.json, which gen_dbt.py emits as ignore_values. Measuring rather
than guessing keeps the test meaningful for the columns that remain in it.
"""

import json
import sys
from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
from constants import CODE_DIR, OUTPUT_DIR, TABLES, waves

THRESHOLD = 0.05
RECENT_WAVES = 12


def main() -> int:
    recent = {(w["year"], w["quarter"]) for w in waves()[-RECENT_WAVES:]}
    report: dict[str, dict] = {}
    sparse: dict[str, list[str]] = {}

    for table in TABLES:
        nonnull_all: dict[str, int] = defaultdict(int)
        nonnull_recent: dict[str, int] = defaultdict(int)
        total_all = total_recent = 0
        for path in sorted(
            (OUTPUT_DIR / table).glob("ano=*/trimestre=*/data.parquet")
        ):
            year = int(path.parts[-3].split("=")[1])
            quarter = int(path.parts[-2].split("=")[1])
            arrow = pq.ParquetFile(path).read()
            n = arrow.num_rows
            total_all += n
            is_recent = (year, quarter) in recent
            if is_recent:
                total_recent += n
            for name, column in zip(
                arrow.schema.names, arrow.columns, strict=True
            ):
                filled = n - column.null_count
                nonnull_all[name] += filled
                if is_recent:
                    nonnull_recent[name] += filled

        rows = []
        for name in nonnull_all:
            share_all = nonnull_all[name] / total_all if total_all else 0.0
            share_recent = (
                nonnull_recent[name] / total_recent if total_recent else 0.0
            )
            rows.append(
                {
                    "column": name,
                    "share_all": round(share_all, 5),
                    "share_recent": round(share_recent, 5),
                }
            )
        rows.sort(key=lambda r: r["share_all"])
        report[table] = {
            "rows": total_all,
            "recent_rows": total_recent,
            "columns": rows,
        }
        # A column is ignored when it is sparse both overall and in the recent
        # window: sparse only in the recent window means it was retired, which
        # the overall share still covers.
        sparse[table] = sorted(
            r["column"]
            for r in rows
            if r["share_all"] < THRESHOLD or r["share_recent"] < THRESHOLD
        )
        print(f"{table}: {total_all:,} rows, {len(rows)} columns")
        print(
            f"   below {THRESHOLD:.0%} overall: "
            f"{sum(1 for r in rows if r['share_all'] < THRESHOLD)}"
        )
        print(
            f"   below {THRESHOLD:.0%} in the last {RECENT_WAVES} waves: "
            f"{sum(1 for r in rows if r['share_recent'] < THRESHOLD)}"
        )
        print(f"   ignore list: {len(sparse[table])} columns")
        print(
            f"   emptiest: {[(r['column'], r['share_all']) for r in rows[:6]]}"
        )

    (CODE_DIR / "null_proportions.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    (CODE_DIR / "sparse_columns.json").write_text(
        json.dumps(sparse, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
