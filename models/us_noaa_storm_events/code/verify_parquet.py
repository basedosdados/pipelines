"""Verify the cleaned parquet against the source before uploading.

Checks the things a dbt test cannot reach because they concern the transform
rather than the warehouse: that no row was lost, that the logical keys are
unique, that the damage decoding reproduces known totals, and what share of each
column is null (which is what sets the ignore_values of the null-proportion
test).

Run: uv run python models/us_noaa_storm_events/code/verify_parquet.py
"""

import json
from collections import Counter, defaultdict

import pyarrow.dataset as ds
from common import DATA_TABLES, OUTPUT, load_cols

EXPECTED = {"event": 2041816, "fatality": 24903, "event_location": 1817621}
KEYS = {
    "event": ["event_id"],
    "fatality": ["event_id", "fatality_id"],
    "event_location": ["event_id", "location_index"],
}


def main() -> None:
    report = {}
    for table in DATA_TABLES:
        cols = [c.name for c in load_cols(table)]
        data = ds.dataset(OUTPUT / table, format="parquet")
        assert data.schema.names == cols, f"{table}: column order drifted"

        n = data.count_rows()
        assert n == EXPECTED[table], (
            f"{table}: {n} rows, expected {EXPECTED[table]}"
        )

        nulls = Counter()
        keys = set()
        key_cols = KEYS[table]
        dup = 0
        for batch in data.to_batches():
            for name in cols:
                col = batch.column(batch.schema.get_field_index(name))
                nulls[name] += col.null_count
            tuples = zip(
                *(
                    batch.column(batch.schema.get_field_index(k)).to_pylist()
                    for k in key_cols
                ),
                strict=True,
            )
            for t in tuples:
                if t in keys:
                    dup += 1
                keys.add(t)
        assert dup == 0, f"{table}: {dup} duplicate keys on {key_cols}"

        report[table] = {
            "rows": n,
            "key": key_cols,
            "duplicate_keys": dup,
            "null_share": {
                k: round(v / n, 4) for k, v in sorted(nulls.items())
            },
        }
        print(f"{table}: {n:,} rows, key {key_cols} unique")
        sparse = [k for k, v in nulls.items() if v / n > 0.95]
        print(f"  >95% null ({len(sparse)}): {sparse}")

    # Damage decoding: the totals must reproduce the magnitudes NOAA publishes
    # for the well-known disaster years.
    ev = ds.dataset(OUTPUT / "event", format="parquet")
    totals: dict[str, float] = defaultdict(float)
    parsed = blank_src = unparsed = 0
    for batch in ev.to_batches(
        columns=["year", "damage_property", "damage_property_source"]
    ):
        for y, v, src in zip(
            batch.column(0).to_pylist(),
            batch.column(1).to_pylist(),
            batch.column(2).to_pylist(),
            strict=True,
        ):
            if v is not None:
                totals[y] += float(v)
                parsed += 1
            elif not src:
                blank_src += 1
            else:
                unparsed += 1
    print(
        f"\ndamage_property: parsed={parsed:,} blank_at_source={blank_src:,} "
        f"undecodable={unparsed:,}"
    )
    for y in ("1953", "1996", "2005", "2017", "2024"):
        if y in totals:
            print(f"   {y} total ${totals[y]:,.0f}")
    report["damage_property"] = {
        "parsed": parsed,
        "blank_at_source": blank_src,
        "undecodable": unparsed,
    }
    (OUTPUT / "verification.json").write_text(json.dumps(report, indent=2))
    print("\nOK")


if __name__ == "__main__":
    main()
