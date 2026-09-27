"""Download and clean every HCRIS hospital extract into partitioned parquet.

    python clean.py                 # every extract in EXTRACTS
    python clean.py 2552-10:2024    # one extract
    python clean.py --skip-download

Writes ``<OUTPUT>/report/year=<YYYY>/`` and ``<OUTPUT>/report_value/year=<YYYY>/``,
all-STRING Snappy parquet. One file per (table, partition, source extract), so
re-cleaning an extract replaces exactly its own files and never orphans a part.

The archives are unpacked one at a time and deleted straight after, so peak disk
stays near one extract (~1.5 GB) rather than the ~16 GB the whole series needs
unpacked at once.
"""

import sys
import time

from common import (
    EXTRACTS,
    INPUT,
    OUTPUT,
    assert_all_string,
    clean_all,
    download_extract,
)


def parse(argv: list[str]) -> tuple[list[tuple[str, int]], bool]:
    """Read the extract selection and the download flag off the command line.

    Args:
        argv: Arguments after the script name.

    Returns:
        ``(extracts, download)``.
    """
    download = "--skip-download" not in argv
    picked = [a for a in argv if not a.startswith("--")]
    if not picked:
        return EXTRACTS, download
    wanted = {(a.split(":")[0], int(a.split(":")[1])) for a in picked}
    return [e for e in EXTRACTS if e in wanted], download


def main() -> None:
    """Download the selected extracts, clean them, and check what was written."""
    extracts, download = parse(sys.argv[1:])
    if download:
        for form, year in extracts:
            download_extract(form, year, INPUT)
        print(f"downloaded {len(extracts)} extracts -> {INPUT}")

    start = time.time()
    totals = clean_all(INPUT, OUTPUT, extracts)
    for table, rows in sorted(totals.items()):
        print(f"{table:>14}: {rows:>12,} rows")
    print(
        f"cleaned {len(extracts)} extracts in {time.time() - start:.0f}s -> {OUTPUT}"
    )

    assert_all_string(OUTPUT)
    print("every parquet is all-STRING and non-empty")


if __name__ == "__main__":
    main()
