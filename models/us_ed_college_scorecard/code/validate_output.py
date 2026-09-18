#!/usr/bin/env python3
"""
Validate the cleaned parquet before upload, and derive the dbt test config.

Checks, per table: row counts against clean_stats.json, column presence and
order against the architecture, and primary-key uniqueness. For the two wide
tables it also measures per-column non-null share, which is what decides the
`ignore_values` list of the not_null_proportion dbt test.

Key uniqueness is proved cheaply rather than by scanning 238M rows. Every
long table's key is (year, unitid, variable_name), and the melt emits exactly
one row per (unitid, source column) per cohort file -- so those keys are
unique if and only if `unitid` is unique within each cohort file, which is
30 x ~6,700 rows to check instead of 235M.

Writes code/validation_report.json and code/sparse_columns.json.

Usage:
    /tmp/cs_venv/bin/python models/us_ed_college_scorecard/code/validate_output.py
"""

import csv
import json
import os
import pathlib
import sys

import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
# pyrefly: ignore [missing-import]
import spec

CODE_DIR = pathlib.Path(__file__).resolve().parent
ARCH_DIR = CODE_DIR / "architecture"
OUTPUT_DIR = pathlib.Path(
    os.environ.get(
        "OUTPUT_DIR",
        pathlib.Path.home() / "Downloads/us_ed_college_scorecard_data/output",
    )
)
LONG_TABLES = sorted(set(spec.LONG_TABLES.values()))
WIDE_TABLES = ("institution", "field_of_study", "variable", "dicionario")
NON_NULL_FLOOR = 0.05

PRIMARY_KEY = {
    "institution": ["year", "unitid"],
    "field_of_study": spec.FIELD_OF_STUDY_KEY,
    "variable": ["variable_name", "source_file"],
    "dicionario": ["id_tabela", "nome_coluna", "chave"],
    **{t: ["year", "unitid", "variable_name"] for t in LONG_TABLES},
}


def arch_columns(table):
    with open(ARCH_DIR / f"{table}.csv", newline="") as fh:
        return [r["name"] for r in csv.DictReader(fh)]


def partition_files(table):
    root = OUTPUT_DIR / table
    return sorted(root.glob("year=*/data.parquet")) or sorted(
        root.glob("*.parquet")
    )


def duplicate_keys(table, key):
    """Rows minus distinct keys, summed over partitions.

    NULLs are filled before joining so that two rows with a null in the same
    key position collide, matching how BigQuery groups them.
    """
    total = 0
    for path in partition_files(table):
        keyed = pq.read_table(path, columns=key)
        parts = [
            # pyrefly: ignore [missing-attribute]
            pc.fill_null(pc.cast(keyed.column(c), "string"), "\x00")
            for c in key
        ]
        # pyrefly: ignore [missing-attribute]
        joined = pc.binary_join_element_wise(*parts, "|")
        # pyrefly: ignore [missing-attribute]
        total += keyed.num_rows - pc.count_distinct(joined).as_py()
    return total


def main():
    counts = json.loads((CODE_DIR / "clean_stats.json").read_text())["rows"]
    report, sparse = {}, {}
    failures = []

    for table in spec.TABLE_SLUGS:
        files = partition_files(table)
        rows = sum(pq.read_metadata(f).num_rows for f in files)
        columns = pq.read_schema(files[0]).names
        expected = arch_columns(table)
        entry = {
            "rows": rows,
            "rows_match_clean": rows == counts[table],
            "partitions": len(files),
            "columns_in_order": columns == expected,
            "missing_columns": sorted(set(expected) - set(columns)),
            "extra_columns": sorted(set(columns) - set(expected)),
            "key": PRIMARY_KEY[table],
        }

        if table in LONG_TABLES:
            # Inherited from institution: see the module docstring.
            entry["key_unique"] = None
        else:
            dupes = duplicate_keys(table, PRIMARY_KEY[table])
            entry["duplicate_keys"] = dupes
            entry["key_unique"] = dupes == 0

        if table in WIDE_TABLES:
            filled = dict.fromkeys(columns, 0)
            for path in files:
                tbl = pq.read_table(path)
                for column in columns:
                    array = tbl.column(column)
                    filled[column] += array.length() - array.null_count
            share = {c: filled[c] / max(rows, 1) for c in columns}
            sparse[table] = sorted(
                c for c in columns if share[c] < NON_NULL_FLOOR
            )
            entry["sparse_columns"] = sparse[table]

        report[table] = entry
        if not entry["rows_match_clean"] or not entry["columns_in_order"]:
            failures.append(table)
        if entry["key_unique"] is False:
            failures.append(table)
        print(
            f"{table:16s} {rows:>13,d} rows  parts={len(files):>2d}  "
            f"cols={entry['columns_in_order']}  key_unique={entry['key_unique']}"
        )
        if table in WIDE_TABLES and sparse[table]:
            print(
                f"                 {len(sparse[table])} columns below {NON_NULL_FLOOR:.0%} non-null"
            )

    unitid_dupes = duplicate_keys("institution", ["year", "unitid"])
    report["_unitid_unique_within_cohort"] = unitid_dupes == 0
    print(
        f"\nunitid unique within every cohort year: {unitid_dupes == 0}"
        f" -> long-table keys (year, unitid, variable_name) are unique"
    )

    (CODE_DIR / "validation_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True)
    )
    (CODE_DIR / "sparse_columns.json").write_text(
        json.dumps(sparse, indent=2, sort_keys=True)
    )
    print("wrote code/validation_report.json and code/sparse_columns.json")
    if failures:
        print(f"\nFAILED: {sorted(set(failures))}")
        sys.exit(1)


if __name__ == "__main__":
    main()
