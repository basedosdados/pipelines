"""One-shot: clean EIA-861 (annual) and EIA-861M (monthly) to partitioned parquet.

    python clean.py                       # every year + monthly + dicionario
    python clean.py --years 2024 2025     # selected annual years
    python clean.py --tables retail_sales # selected tables
    python clean.py --download            # fetch missing ZIPs and the 861M file

Writes ``output/<table>/year=<year>/data.parquet`` and
``output/dicionario/data.parquet``. The transform is
``pipelines.datasets.us_eia_consumption.utils.clean_all`` — the same the flow
calls — so this is a thin driver.
"""

import argparse
import time

from common import (
    DATA_TABLES,
    INPUT,
    OUTPUT,
    assert_all_string,
    build_dicionario,
    clean_all,
    download_eia861m,
    download_year,
)

from pipelines.datasets.us_eia_consumption.constants import constants


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--years", type=int, nargs="*")
    parser.add_argument("--tables", nargs="*", choices=DATA_TABLES)
    parser.add_argument("--download", action="store_true")
    parser.add_argument("--no-dicionario", action="store_true")
    args = parser.parse_args()

    if args.download:
        years = args.years or list(
            range(constants.FIRST_ANNUAL_YEAR.value, 2026)
        )
        got = [y for y in years if download_year(y, INPUT)]
        print(f"annual ZIPs available: {len(got)} ({min(got)}-{max(got)})")
        download_eia861m(INPUT)
        print("downloaded sales_revenue.xlsx")

    started = time.time()
    totals = clean_all(INPUT, OUTPUT, years=args.years, tables=args.tables)
    print("\n=== totals ===")
    for table, rows in sorted(totals.items()):
        print(f"{table:22s} {rows:>12,}")

    if not args.no_dicionario:
        n = build_dicionario(OUTPUT)
        print(f"{'dicionario':22s} {n:>12,}")

    for table in (*(args.tables or DATA_TABLES), "dicionario"):
        if (OUTPUT / table).exists():
            assert_all_string(OUTPUT / table)
    print(
        f"\nall partitions all-STRING and non-empty; {time.time() - started:.0f}s"
    )


if __name__ == "__main__":
    main()
