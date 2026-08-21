"""Profile the cleaned parquet: null shares, cardinality, and value sets.

Three later steps depend on this rather than on guesswork:

* ``gen_dbt`` needs to know which columns are legitimately sparse, so the
  ``not_null_proportion_multiple_columns`` test can exempt them by name
  instead of being weakened for every column.
* ``gen_dicionario`` needs the distinct value set of each dictionary column.
* The dictionary flag itself is re-checked here: anything that turns out to
  hold free text rather than a bounded value set is reported for demotion.

    uv run --with duckdb python profile_data.py
"""

import json

import constants as c
import layout
import schema

# Above this many distinct values a column is not a value set worth writing
# into the dictionary table. Open Payments stores readable labels rather than
# codes, so the dictionary here serves as a Portuguese and Spanish gloss of
# each permitted value -- worth doing for the 2-to-15-value columns, not for
# the specialty fields, which carry roughly a thousand NUCC taxonomy paths.
DICTIONARY_MAX_DISTINCT = 100

# Columns below this non-null share are exempted from the dbt null-proportion
# test, which otherwise fails on columns that are null by design -- the
# teaching hospital fields on a physician payment, say, or product slot 5.
SPARSE_THRESHOLD = 0.05

PROFILE_PATH = c.DATA_ROOT / "profile.json"


def _source(table: str) -> str:
    root = c.OUTPUT_DIR / table
    if table in layout.UNPARTITIONED:
        return f"read_parquet('{root}/data.parquet')"
    return f"read_parquet('{root}/*/data.parquet')"


def profile_table(con, table: str) -> dict:
    columns = layout.LAYOUT[table]
    src = _source(table)
    total = con.execute(f"SELECT count(*) FROM {src}").fetchone()[0]
    if total == 0:
        return {"rows": 0, "non_null_share": {}, "distinct": {}, "values": {}}

    non_null = ", ".join(f'count("{col}") AS "{col}"' for col in columns)
    counts = dict(
        zip(
            columns,
            con.execute(f"SELECT {non_null} FROM {src}").fetchone(),
            strict=True,
        )
    )

    dictionary_columns = [
        col
        for col in columns
        if schema.covered_by_dictionary(table, col) == "yes"
    ]
    distinct, values = {}, {}
    for col in dictionary_columns:
        n = con.execute(
            f'SELECT count(DISTINCT "{col}") FROM {src}'
        ).fetchone()[0]
        distinct[col] = n
        if n <= DICTIONARY_MAX_DISTINCT:
            values[col] = [
                r[0]
                for r in con.execute(
                    f'SELECT DISTINCT "{col}" FROM {src} WHERE "{col}" IS NOT NULL ORDER BY 1'
                ).fetchall()
            ]

    return {
        "rows": total,
        "non_null_share": {col: counts[col] / total for col in columns},
        "distinct": distinct,
        "values": values,
    }


def main() -> None:
    # Imported here so gen_dbt can reuse this module's constants without duckdb.
    import duckdb

    con = duckdb.connect(
        config={"memory_limit": "8GB", "preserve_insertion_order": "false"}
    )
    con.execute(f"SET temp_directory='{c.DATA_ROOT / 'duckdb_tmp'}'")

    out = {}
    for table in layout.LAYOUT:
        if table == "dicionario":
            continue
        root = c.OUTPUT_DIR / table
        if not root.exists():
            print(f"  {table:38s} not cleaned yet, skipped")
            continue
        out[table] = profile_table(con, table)
        sparse = [
            col
            for col, share in out[table]["non_null_share"].items()
            if share < SPARSE_THRESHOLD
        ]
        print(
            f"  {table:38s} {out[table]['rows']:>12,} rows, {len(sparse):>3d} sparse columns"
        )
    con.close()

    with open(PROFILE_PATH, "w") as fh:
        json.dump(out, fh, indent=1)

    print(
        "\ndictionary columns exceeding the distinct-value ceiling (demote these):"
    )
    flagged = False
    for table, prof in out.items():
        for col, n in prof["distinct"].items():
            if n > DICTIONARY_MAX_DISTINCT:
                print(f"  {table}.{col}: {n:,} distinct")
                flagged = True
    if not flagged:
        print("  none")
    print(f"\nprofile written to {PROFILE_PATH}")


if __name__ == "__main__":
    main()
