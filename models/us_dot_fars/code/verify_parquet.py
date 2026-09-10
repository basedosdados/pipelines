"""Measure the cleaned parquet: row counts, key uniqueness, null shares, ranges.

Everything the dbt tests will assert is checked here first, against the local
parquet, so a failure is diagnosed in seconds instead of after a BigQuery load.
It also reports which columns fall below the 0.05 non-null floor of
not_null_proportion_multiple_columns, which is the list that has to be excluded
from that test in schema.yml.

Run: uv run python models/us_dot_fars/code/verify_parquet.py
"""

import json

import pyarrow.compute as pc
import pyarrow.dataset as ds
from common import ALL_TABLES, DATA_TABLES, OUTPUT, load_cols

KEYS = {
    "crash": ["year", "state_id", "case_number"],
    "vehicle": ["year", "state_id", "case_number", "vehicle_number"],
    "person": [
        "year",
        "state_id",
        "case_number",
        "vehicle_number",
        "person_number",
    ],
}

# Bounds a value must satisfy to be physically possible. These exist to catch a
# sentinel code surviving the transform, which is the failure mode that matters
# here: FARS sentinels are dense integers sitting in the same range as real
# values, so a leak is silent and looks like data.
RANGES = {
    ("crash", "month"): (1, 12),
    ("crash", "day"): (1, 31),
    ("crash", "hour"): (0, 23),
    ("crash", "minute"): (0, 59),
    ("crash", "latitude"): (17.0, 72.0),
    ("crash", "longitude"): (-180.0, -64.0),
    ("crash", "speed_limit"): (5, 95),
    ("vehicle", "model_year"): (1900, 2030),
    ("vehicle", "travel_speed"): (0, 151),
    ("vehicle", "vehicle_speed_limit"): (0, 95),
    # Widened to the source's own spread. NHTSA publishes a thin tail of
    # implausible driver heights and weights (a 1 lb driver, a 104 in driver)
    # that are not documented sentinel codes, so they are carried through rather
    # than silently rewritten; the counts are reported in the column notes.
    ("vehicle", "driver_height"): (20, 110),
    ("vehicle", "driver_weight"): (1, 800),
    ("person", "age"): (0, 120),
    ("person", "blood_alcohol_content"): (0.0, 0.94),
    ("person", "death_hour"): (0, 23),
    ("person", "death_minute"): (0, 59),
}


def main() -> None:
    sparse: dict[str, list[str]] = {}
    failures = []

    for table in ALL_TABLES:
        d = ds.dataset(OUTPUT / table, format="parquet")
        n = d.count_rows()
        print(f"\n=== {table}: {n:,} rows ===")

        if table in DATA_TABLES:
            key = d.to_table(columns=KEYS[table])
            tuples = set(
                zip(
                    *[key.column(k).to_pylist() for k in KEYS[table]],
                    strict=True,
                )
            )
            dup = n - len(tuples)
            print(f"  key {KEYS[table]}: {dup} duplicate(s)")
            if dup:
                failures.append(f"{table}: {dup} duplicate keys")

        low = []
        for c in load_cols(table):
            col = d.to_table(columns=[c.name]).column(c.name)
            nn = n - col.null_count
            share = nn / n if n else 0.0
            if share < 0.05:
                low.append(c.name)
            flag = ""
            if (table, c.name) in RANGES and nn:
                lo, hi = RANGES[(table, c.name)]
                vals = pc.cast(col.drop_null(), "double")
                mn, mx = pc.min(vals).as_py(), pc.max(vals).as_py()
                if mn < lo or mx > hi:
                    flag = f"   <-- OUT OF RANGE [{lo}, {hi}]: {mn} .. {mx}"
                    failures.append(
                        f"{table}.{c.name} out of range: {mn}..{mx}"
                    )
                else:
                    flag = f"   range {mn} .. {mx}"
            print(f"  {c.name:44s} non-null {share:6.1%}{flag}")
        if low:
            sparse[table] = low

    # custom_dictionary_coverage fails on any non-null value with no matching
    # chave in dicionario for that (table, column). It compares codes only, not
    # eras, so a code documented in any year passes. Checked here because a
    # failure is a schema decision - either the column is not really dictionary
    # material, or the source never published a label set for it - and finding
    # that out from a dbt run against BigQuery is slow and expensive.
    print(
        "\n=== dictionary coverage (what custom_dictionary_coverage asserts) ==="
    )
    dic = (
        ds.dataset(OUTPUT / "dicionario", format="parquet")
        .to_table()
        .to_pylist()
    )
    keys: dict[tuple[str, str], set[str]] = {}
    for r in dic:
        keys.setdefault((r["id_tabela"], r["nome_coluna"]), set()).add(
            r["chave"]
        )
    for table in DATA_TABLES:
        d = ds.dataset(OUTPUT / table, format="parquet")
        for c in load_cols(table):
            if not c.covered_by_dictionary:
                continue
            vals = {
                v
                for v in d.to_table(columns=[c.name])
                .column(c.name)
                .to_pylist()
                if v is not None
            }
            missing = vals - keys.get((table, c.name), set())
            status = (
                "OK"
                if not missing
                else f"{len(missing)} UNCOVERED of {len(vals)}"
            )
            print(f"  {table}.{c.name:44s} {status}")
            if missing:
                sample = sorted(missing)[:8]
                print(f"      e.g. {sample}")
                failures.append(
                    f"{table}.{c.name}: {len(missing)} values not in dicionario"
                )

    print("\n=== columns below the 0.05 non-null floor ===")
    print(json.dumps(sparse, indent=2))
    print("\n=== FAILURES ===" if failures else "\nno failures")
    for f in failures:
        print("  " + f)


if __name__ == "__main__":
    main()
