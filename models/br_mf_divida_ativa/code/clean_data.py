#!/usr/bin/env python3
"""Bootstrap: download + clean PGFN Dívida Ativa quarters into partitioned
Parquet under the data root (outside Dropbox - see ``divida_ativa.data_root``).

The transform lives in ``divida_ativa.py`` so the recurring pipeline and this
one-shot bootstrap share one implementation. Downloads one quarter at a time and
(by default) deletes each ZIP after cleaning, so peak disk stays near one ZIP.

Usage:
    # one quarter, all three tables
    uv run python models/br_mf_divida_ativa/code/clean_data.py --quarters 2026Q2

    # full backfill 2020Q1..2026Q2 (all tables), keep going past missing quarters
    uv run python models/br_mf_divida_ativa/code/clean_data.py --all

    # a subset of tables
    uv run python models/br_mf_divida_ativa/code/clean_data.py --quarters 2025Q4 --tables fgts previdenciario
"""

import argparse
import logging
import re
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
# pyrefly: ignore [missing-import]
from divida_ativa import (
    TABLES,
    clean_quarter_zip,
    data_root,
    download_quarter,
    source_exists,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("br_mf_divida_ativa")

FIRST_YEAR, FIRST_Q = 2020, 1
LAST_YEAR, LAST_Q = 2026, 2


def all_quarters() -> list[tuple[int, int]]:
    """Every (year, quarter) from FIRST_YEAR/FIRST_Q to LAST_YEAR/LAST_Q.

    Returns:
        Ordered list of ``(year, quarter)`` pairs spanning the full backfill.
    """
    out = []
    y, q = FIRST_YEAR, FIRST_Q
    while (y, q) <= (LAST_YEAR, LAST_Q):
        out.append((y, q))
        q += 1
        if q > 4:
            y, q = y + 1, 1
    return out


def parse_quarters(tokens: list[str]) -> list[tuple[int, int]]:
    """Parse quarter tokens such as ``2026Q2``.

    Args:
        tokens: Quarter tokens in ``YYYYQ[1-4]`` form (case-insensitive Q).

    Returns:
        A list of ``(year, quarter)`` pairs.

    Raises:
        SystemExit: If a token is not a valid quarter (only Q1-Q4 accepted).
    """
    out = []
    for t in tokens:
        m = re.fullmatch(r"(\d{4})[Qq]([1-4])", t.strip())
        if not m:
            raise SystemExit(f"bad quarter token {t!r}; expected e.g. 2026Q2")
        out.append((int(m.group(1)), int(m.group(2))))
    return out


def main() -> None:
    """Download and clean the requested quarters into partitioned Parquet.

    Parses CLI args (``--all`` or ``--quarters``, ``--tables``, ``--keep-zip``,
    ``--skip-existing``), then downloads and cleans each (quarter, table) into
    the data root. Failures are collected and reported; the process exits
    non-zero if any occurred, so a wrapper or scheduler can detect a partial
    backfill.
    """
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument(
        "--all", action="store_true", help="full 2020Q1..2026Q2 backfill"
    )
    g.add_argument(
        "--quarters", nargs="+", help="explicit quarters, e.g. 2026Q2 2025Q4"
    )
    ap.add_argument(
        "--tables", nargs="+", default=list(TABLES), choices=TABLES
    )
    ap.add_argument(
        "--keep-zip",
        action="store_true",
        help="do not delete ZIPs after cleaning",
    )
    ap.add_argument(
        "--skip-existing",
        action="store_true",
        help="skip a (quarter, table) whose output partition already has parquet (resumable)",
    )
    args = ap.parse_args()

    quarters = all_quarters() if args.all else parse_quarters(args.quarters)
    root = data_root()
    input_dir = root / "input"
    output_dir = root / "output"
    session = requests.Session()

    log.info("data root: %s", root)
    log.info("quarters: %s | tables: %s", quarters, args.tables)

    grand = {t: 0 for t in args.tables}
    failures = []
    for year, quarter in quarters:
        for table in args.tables:
            try:
                if args.skip_existing:
                    pdir = (
                        output_dir
                        / table
                        / f"ano={year}"
                        / f"trimestre={quarter}"
                    )
                    if any(pdir.glob("*.parquet")):
                        log.info(
                            "SKIP %s %sQ%s - output already present",
                            table,
                            year,
                            quarter,
                        )
                        continue
                if not source_exists(year, quarter, table, session):
                    log.warning(
                        "SKIP %s %sQ%s - source not found (404)",
                        table,
                        year,
                        quarter,
                    )
                    continue
                zip_path = download_quarter(
                    year, quarter, table, input_dir, session
                )
                try:
                    n = clean_quarter_zip(
                        zip_path, table, year, quarter, output_dir
                    )
                    grand[table] += n
                finally:
                    if not args.keep_zip:
                        zip_path.unlink(missing_ok=True)
            except Exception as e:
                # keep going: one bad quarter shouldn't abort a multi-hour run.
                log.error(
                    "FAILED %s %sQ%s: %s: %s",
                    table,
                    year,
                    quarter,
                    type(e).__name__,
                    e,
                )
                failures.append(
                    f"{table} {year}Q{quarter}: {type(e).__name__}: {e}"
                )
    log.info("=== DONE. rows by table: %s ===", grand)
    if failures:
        log.error(
            "=== %d FAILURE(S) - rerun these with --quarters ===",
            len(failures),
        )
        for f in failures:
            log.error("  %s", f)
        sys.exit(1)


if __name__ == "__main__":
    main()
