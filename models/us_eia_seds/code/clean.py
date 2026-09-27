"""One-shot: clean the whole SEDS series to partitioned parquet.

    python clean.py                 # every year + dicionario
    python clean.py --years 2024    # selected years
    python clean.py --download      # fetch Complete_SEDS.csv + codes first

Reads ``$US_EIA_SEDS_DATA_DIR/input/Complete_SEDS.csv`` and writes
``output/seds_consumption/year=<year>/data.parquet`` plus
``output/dicionario/data.parquet``.

The transform is ``pipelines.datasets.us_eia_seds.utils.clean_all`` — the same
function the recurring flow calls — so this is a thin driver.
"""

import argparse
import time

from common import (
    INPUT,
    OUTPUT,
    assert_all_string,
    build_dicionario,
    clean_all,
    download_codes,
    download_complete,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--years", type=int, nargs="*")
    parser.add_argument(
        "--download", action="store_true", help="fetch source files first"
    )
    parser.add_argument(
        "--no-dicionario",
        action="store_true",
        help="skip rebuilding dicionario",
    )
    args = parser.parse_args()

    if args.download:
        print("downloading Complete_SEDS.csv ...")
        download_complete(INPUT)
        print("downloading Codes_and_Descriptions.xlsx ...")
        download_codes(INPUT)

    started = time.time()
    totals = clean_all(INPUT, OUTPUT, years=args.years)
    print("\n=== totals ===")
    for table, rows in sorted(totals.items()):
        print(f"{table:24s} {rows:>12,}")

    if not args.no_dicionario:
        n = build_dicionario(OUTPUT)
        print(f"{'dicionario':24s} {n:>12,}")

    for table in ("seds_consumption", "dicionario"):
        assert_all_string(OUTPUT / table)
    print(
        f"\nall partitions all-STRING and non-empty; {time.time() - started:.0f}s"
    )


if __name__ == "__main__":
    main()
