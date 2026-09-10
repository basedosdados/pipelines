"""Clean every us_census_cog source file into partitioned parquet.

    python clean.py                          # every table, every year
    python clean.py government_unit          # one table
    python clean.py finance 2017 2018        # one table, selected years

Output lands in ``$CENSUS_COG_DATA_DIR/output/<table>/year=<Y>/data.parquet``,
all-string, one file per partition. The dicionario is written last because it
reads back the special-district function names from the cleaned output.
"""

import sys
import time

from common import DATA_TABLES, INPUT, OUTPUT

from pipelines.datasets.us_census_cog.utils import (
    build_dicionario,
    clean_table_year,
    collect_function_labels,
    table_years,
    write_partitioned,
)


def main(argv: list[str]) -> None:
    """Clean the requested tables and years."""
    tables = [a for a in argv if not a.isdigit()] or list(DATA_TABLES)
    only_years = {int(a) for a in argv if a.isdigit()}

    totals: dict[str, int] = {}
    for table in tables:
        years = [
            y for y in table_years(table) if not only_years or y in only_years
        ]
        total = 0
        started = time.monotonic()
        for year in years:
            rows = clean_table_year(table, INPUT, OUTPUT, year)
            total += rows
            print(f"  {table} {year}: {rows:,} rows", flush=True)
        totals[table] = total
        elapsed = time.monotonic() - started
        print(f"{table}: {total:,} rows in {len(years)} years, {elapsed:.0f}s")

    if not only_years and set(tables) >= {"government_unit"}:
        rows = build_dicionario(collect_function_labels(OUTPUT))
        written = write_partitioned(rows, OUTPUT, "dicionario")
        print(f"dicionario: {written:,} rows")

    print({k: f"{v:,}" for k, v in totals.items()})


if __name__ == "__main__":
    main(sys.argv[1:])
