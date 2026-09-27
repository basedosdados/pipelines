"""One-shot: clean every EIA-860 and EIA-923 report year to partitioned parquet.

    python clean.py                       # every year, every table
    python clean.py --years 2024 2025     # selected years
    python clean.py --tables plant        # selected tables

Reads the ZIPs already under ``$US_EIA_ELECTRICITY_DATA_DIR/input`` (``--download`` fetches
any that are missing) and writes ``output/<table>/year=<year>/data.parquet``.

The transform itself is ``pipelines.datasets.us_eia_electricity.utils.clean_all`` — the same
function the recurring flow calls — so this script is a thin driver, not a second
implementation.
"""

import argparse
import time

from common import (
    DATA_TABLES,
    INPUT,
    OUTPUT,
    assert_all_string,
    clean_all,
    download_form,
)


def main() -> None:
    """Clean the requested report years to partitioned parquet.

    Reads ``--years`` / ``--tables`` from the command line (default: every year
    and every table found under the input directory), optionally fetching any
    missing source ZIPs first with ``--download``. Writes
    ``output/<table>/year=<year>/data.parquet`` and then asserts every partition
    is all-STRING and non-empty.

    Raises:
        AssertionError: If any written partition is empty or carries a
            non-string column, either of which would poison the staging schema.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--years", type=int, nargs="*")
    parser.add_argument("--tables", nargs="*", choices=DATA_TABLES)
    parser.add_argument(
        "--download",
        action="store_true",
        help="fetch missing source ZIPs first",
    )
    args = parser.parse_args()

    if args.download:
        for form in ("eia860", "eia923"):
            paths = download_form(form, INPUT, years=args.years)
            print(f"{form}: {len(paths)} year(s) available")

    started = time.time()
    totals = clean_all(INPUT, OUTPUT, years=args.years, tables=args.tables)
    print("\n=== totals ===")
    for table, rows in sorted(totals.items()):
        print(f"{table:24s} {rows:>12,}")
    for table in totals:
        assert_all_string(OUTPUT / table)
    print(
        f"\nall partitions all-STRING and non-empty; {time.time() - started:.0f}s"
    )


if __name__ == "__main__":
    main()
