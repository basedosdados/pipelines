"""Measure the cleaned us_census_cog parquet before it goes anywhere.

    python validate.py                 # every table
    python validate.py finance

Reports, per table: row counts by year, the non-null share of every column, the
uniqueness of each declared key, and the share of geographic identifiers absent
from the Data Basis US directories. The last two feed the thresholds in
gen_dbt.py, so they are measured rather than guessed.

Null counts come from the parquet row-group statistics and cost no scan.
Everything else runs through arrow compute rather than Python loops, which
matters on a 116-million-row table.

Each key begins with ``year`` and every file holds exactly one year, so
uniqueness within a file implies uniqueness across the table.

The directory tables are small and are pulled from BigQuery once into a local
cache next to the output.
"""

import json
import sys
from collections import Counter

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from common import DATA_TABLES, OUTPUT

from pipelines.datasets.us_census_cog.utils import load_cols

CACHE = OUTPUT.parent / "directory_cache.json"
DIRECTORIES = {
    "state_id": ("br_bd_diretorios_us.state", "id_state"),
    "county_id": ("br_bd_diretorios_us.county", "id_county"),
    "place_id": ("br_bd_diretorios_us.place", "id_place"),
}
# county_subdivision_id has no directory to check against.
KEYS = {
    "government_unit": [
        ("government_id", ["government_id"]),
        ("government_id_govs", ["government_id_govs"]),
    ],
    "employment": [
        ("government_id_govs", ["government_id_govs", "function_code"])
    ],
    "employment_unit": [("government_id_govs", ["government_id_govs"])],
    "finance": [
        ("government_id_govs", ["government_id_govs", "item_code"]),
        ("government_id", ["government_id", "item_code"]),
    ],
    "finance_unit": [
        ("government_id_govs", ["government_id_govs"]),
        ("government_id", ["government_id"]),
    ],
}


def directory_values() -> dict[str, list[str]]:
    """Fetch the directory key columns, caching them next to the output."""
    if CACHE.exists():
        return json.loads(CACHE.read_text())
    sys.path.insert(
        0, "/Users/rdahis/Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp"
    )
    import server

    out = {}
    for column, (table, field) in DIRECTORIES.items():
        result = server.query_bigquery(
            sql=f"select {field} from `basedosdados.{table}`",
            billing_project="basedosdados-dev",
        )
        out[column] = [str(r[field]) for r in result["rows"] if r[field]]
        print(f"  directory {table}: {len(out[column]):,} keys", flush=True)
    CACHE.write_text(json.dumps(out))
    return out


def check(table: str, directories: dict[str, list[str]]) -> None:
    """Measure one table and print the findings."""
    columns = [c.name for c in load_cols(table)]
    files = sorted((OUTPUT / table).rglob("data.parquet"))
    if not files:
        print(f"{table}: no output")
        return

    total = 0
    nulls: Counter = Counter()
    by_year: dict[int, int] = {}
    duplicates: Counter = Counter()
    keyed: Counter = Counter()
    missing: Counter = Counter()
    present: Counter = Counter()

    for path in files:
        year = (
            int(path.parent.name.split("=")[1])
            if "=" in path.parent.name
            else 0
        )
        parquet = pq.ParquetFile(path)
        metadata = parquet.metadata
        total += metadata.num_rows
        by_year[year] = by_year.get(year, 0) + metadata.num_rows
        for group in range(metadata.num_row_groups):
            for index, name in enumerate(parquet.schema_arrow.names):
                statistics = metadata.row_group(group).column(index).statistics
                if statistics is not None:
                    nulls[name] += statistics.null_count

        wanted = sorted(
            {c for c in DIRECTORIES if c in columns}
            | {c for _, key in KEYS.get(table, []) for c in key}
        )
        if not wanted:
            continue
        data = parquet.read(columns=wanted)
        for column in DIRECTORIES:
            if column not in columns:
                continue
            values = data.column(column).combine_chunks()
            valid = pc.drop_null(values)
            present[column] += len(valid)
            if not len(valid):
                continue
            known = pc.is_in(
                valid,
                value_set=pa.array(directories[column], type=pa.string()),
            )
            missing[column] += len(valid) - (pc.sum(known).as_py() or 0)
        for label, key in KEYS.get(table, []):
            subset = data.select(key)
            subset = subset.filter(pc.is_valid(subset.column(label)))
            if subset.num_rows == 0:
                continue
            distinct = subset.group_by(key).aggregate([]).num_rows
            keyed[label] += subset.num_rows
            duplicates[label] += subset.num_rows - distinct

    print(
        f"\n### {table}: {total:,} rows in {len(files)} partitions", flush=True
    )
    print(
        "   years:",
        ", ".join(f"{y}={n:,}" for y, n in sorted(by_year.items())),
    )
    sparse = [
        f"{c}={1 - nulls[c] / total:.4f}"
        for c in columns
        if total and (1 - nulls[c] / total) < 0.05
    ]
    print("   below the 5% non-null floor:", ", ".join(sparse) or "none")
    near = [
        f"{c}={1 - nulls[c] / total:.4f}"
        for c in columns
        if total and 0.05 <= (1 - nulls[c] / total) < 0.20
    ]
    print("   within reach of the floor (5-20%):", ", ".join(near) or "none")
    for label, _ in KEYS.get(table, []):
        if not keyed[label]:
            print(f"   key on {label}: no rows carry it")
            continue
        share = duplicates[label] / keyed[label]
        print(
            f"   key on {label}: {keyed[label]:,} rows, "
            f"{duplicates[label]:,} duplicates ({share:.6f})"
        )
    for column in DIRECTORIES:
        if column in columns and present[column]:
            share = missing[column] / present[column]
            print(
                f"   {column}: {present[column]:,} values, "
                f"{missing[column]:,} not in the directory ({share:.6f})"
            )


def main(tables: list[str]) -> None:
    """Validate the requested tables."""
    directories = directory_values()
    for table in tables or list(DATA_TABLES):
        check(table, directories)


if __name__ == "__main__":
    main(sys.argv[1:])
