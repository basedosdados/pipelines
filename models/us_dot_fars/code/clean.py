"""Clean every FARS year into partitioned parquet, then build the dicionario.

Run: uv run python models/us_dot_fars/code/clean.py [first_year] [last_year]

Downloads what is missing into ``INPUT`` and writes
``OUTPUT/<table>/year=<Y>/data.parquet``. The transform itself lives in
``pipelines/datasets/us_dot_fars/utils.py`` and is shared with the recurring
pipeline.
"""

import sys
import time

from common import (
    ALL_TABLES,
    DATA_TABLES,
    FIRST_YEAR,
    INPUT,
    OUTPUT,
    assert_all_string,
    build_dicionario,
    clean_year,
    latest_published_year,
)


def main() -> None:
    first = int(sys.argv[1]) if len(sys.argv) > 1 else FIRST_YEAR
    last = (
        int(sys.argv[2]) if len(sys.argv) > 2 else latest_published_year(first)
    )
    years = list(range(first, last + 1))
    INPUT.mkdir(parents=True, exist_ok=True)
    OUTPUT.mkdir(parents=True, exist_ok=True)

    totals = dict.fromkeys(DATA_TABLES, 0)
    for year in years:
        t0 = time.time()
        counts = clean_year(year, INPUT, OUTPUT)
        for table, n in counts.items():
            totals[table] += n
        print(
            f"  {year}: "
            + "  ".join(f"{t}={n:,}" for t, n in counts.items())
            + f"   ({time.time() - t0:.0f}s)",
            flush=True,
        )

    print(
        "\nbuilding dicionario from NHTSA's own per-year code sets ...",
        flush=True,
    )
    n = build_dicionario(years, INPUT, OUTPUT)
    print(f"  dicionario: {n:,} rows", flush=True)

    for table in ALL_TABLES:
        assert_all_string(OUTPUT / table)
    print("\nall parquet columns are STRING (staging convention)")
    print("TOTALS: " + "  ".join(f"{t}={n:,}" for t, n in totals.items()))


if __name__ == "__main__":
    main()
