"""List the stop columns filled by fewer than 5% of stops since 2016.

The dbt ``not_null_proportion_multiple_columns`` test runs on ``year >= 2016``
at a 5% threshold; the columns printed here go in ``gen_dbt.SPARSE_STOP_COLUMNS``.

Usage:
    python models/us_stanford_open_policing/code/measure_sparsity.py
"""

import duckdb

try:
    from models.us_stanford_open_policing.code import architecture as arch
    from models.us_stanford_open_policing.code import constants
except ModuleNotFoundError:  # run as a script
    import architecture as arch  # type: ignore[no-redef]
    import constants  # type: ignore[no-redef]

THRESHOLD = 0.05
FROM_YEAR = 2016


def main() -> None:
    glob = constants.OUTPUT_DIR / "stop" / "**" / "*.parquet"
    # year and state_abbreviation are hive partitions, never null when set
    cols = [
        c
        for c in arch.column_names("stop")
        if c not in ("year", "state_abbreviation")
    ]
    exprs = ", ".join(f'count("{c}")' for c in cols)
    row = duckdb.sql(
        f"select count(*), {exprs} from read_parquet('{glob}', "
        "hive_partitioning = true, union_by_name = true) "
        f"where try_cast(year as int) >= {FROM_YEAR}"
    ).fetchone()
    assert row is not None
    n, *counts = row
    print(f"stops since {FROM_YEAR}: {n:,}")
    for c, k in sorted(zip(cols, counts, strict=True), key=lambda x: x[1]):
        if k / n < THRESHOLD:
            print(f"{c}: {k / n:.4f}")


if __name__ == "__main__":
    main()
