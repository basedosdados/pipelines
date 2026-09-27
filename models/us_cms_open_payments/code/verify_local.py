"""Check the cleaned parquet before anything is uploaded.

Four things, each of which has already gone wrong at least once in some
onboarding: the staging schema is not all-STRING, a partition is missing, the
declared grain is not actually unique, or a dictionary column holds values the
glossary does not cover.

    uv run --with duckdb python verify_local.py
"""

import json

import constants as c
import grains
import layout
import profile_data
import pyarrow.parquet as pq
import schema as col_schema
from glossary import gloss


def main() -> None:
    import duckdb

    con = duckdb.connect(
        config={"memory_limit": "8GB", "preserve_insertion_order": "false"}
    )
    con.execute(f"SET temp_directory='{c.DATA_ROOT / 'duckdb_tmp'}'")
    problems = []

    print(f"{'table':38s} {'rows':>13s}  {'files':>5s}  grain")
    for table in layout.LAYOUT:
        root = c.OUTPUT_DIR / table
        if not root.exists():
            problems.append(f"{table}: no output directory")
            continue

        files = sorted(root.rglob("*.parquet"))
        expected = (
            1 if table in layout.UNPARTITIONED else len(layout.COVERAGE[table])
        )
        partitions = [f for f in files if f.name != "00_header.parquet"]
        if len(partitions) != expected:
            problems.append(
                f"{table}: {len(partitions)} partition file(s), expected {expected}"
            )

        for path in files:
            types = {str(t) for t in pq.read_schema(path).types}
            if types != {"string"}:
                problems.append(
                    f"{table}/{path.name}: non-string columns {types - {'string'}}"
                )
            names = pq.read_schema(path).names
            if names != layout.LAYOUT[table]:
                problems.append(
                    f"{table}/{path.name}: column order differs from the layout"
                )

        source = (
            f"read_parquet('{root}/data.parquet')"
            if table in layout.UNPARTITIONED
            else f"read_parquet('{root}/*/data.parquet')"
        )
        key = ", ".join(f'"{k}"' for k in grains.GRAIN[table])
        rows, unique = con.execute(
            f"SELECT count(*), count(DISTINCT ({key})) FROM {source}"
        ).fetchone()
        verdict = (
            "unique" if rows == unique else f"DUPLICATES ({rows - unique:,})"
        )
        if rows != unique:
            problems.append(
                f"{table}: grain {grains.GRAIN[table]} is not unique"
            )
        print(f"{table:38s} {rows:>13,}  {len(files):>5d}  {verdict}")

    if profile_data.PROFILE_PATH.exists():
        with open(profile_data.PROFILE_PATH) as fh:
            prof = json.load(fh)
        for table, data in prof.items():
            for column, values in data.get("values", {}).items():
                if col_schema.covered_by_dictionary(table, column) != "yes":
                    continue
                missing = [v for v in values if gloss(column, v) is None]
                if missing:
                    problems.append(
                        f"{table}.{column}: {len(missing)} value(s) without a gloss: {missing[:3]}"
                    )
    con.close()

    print()
    if problems:
        print(f"{len(problems)} problem(s):")
        for problem in problems:
            print(f"  {problem}")
        raise SystemExit(1)
    print("all checks passed")


if __name__ == "__main__":
    main()
