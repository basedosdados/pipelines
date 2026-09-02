#!/usr/bin/env python3
"""One-shot cleaning entrypoint for us_ed_nces_ccd.

Downloads the Urban Institute bulk CSVs and writes year-partitioned, all-STRING
Parquet under ``$CCD_DATA_DIR/output/<table>/year=<year>/data.parquet``.

The transform itself lives in ``utils.py`` and is shared with the recurring
Prefect pipeline, so the two cannot diverge.

The enrollment extract is 21 GB of CSV across 39 files. It is handled one year
at a time and each source file is deleted immediately after its partition is
written, so peak disk stays near a single year rather than the whole series.

Usage:
    uv run --no-project --with duckdb python models/us_ed_nces_ccd/code/clean_data.py
    ... --tables school,staff --years 2020,2021
    ... --keep-csv          # do not delete enrollment CSVs after conversion
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

# pyrefly: ignore [missing-import]
import schema

# pyrefly: ignore [missing-import]
import utils

ROOT = Path(__file__).resolve().parent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("clean")

ALL_TABLES = [
    "school",
    "school_district",
    "staff",
    "district_finance",
    "school_enrollment",
    "dicionario",
]


def finance_table() -> schema.Table:
    header = next(
        csv.reader(
            (utils.input_dir() / "districts_ccd_finance.csv").open(
                encoding="utf-8"
            )
        )
    )
    labels = {
        x["variable"]: x["label"]
        for x in json.loads((ROOT / "varlist_29.json").read_text())
    }
    return schema.finance_table(header, labels)


def source_years(con, path: Path) -> list[int]:
    rows = con.execute(
        f"select distinct cast(try_cast(year as double) as int) y "
        f"from {utils._read(path)} where year is not null order by 1"
    ).fetchall()
    return [r[0] for r in rows if r[0] is not None]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tables", default=",".join(ALL_TABLES))
    ap.add_argument("--years", default="")
    ap.add_argument("--keep-csv", action="store_true")
    ap.add_argument("--memory", default="6GB")
    args = ap.parse_args()

    tables = [t.strip() for t in args.tables.split(",") if t.strip()]
    only_years = {int(y) for y in args.years.split(",") if y.strip()} or None

    con = utils.connect(memory_limit=args.memory)
    out = utils.output_dir()
    counts: dict[str, int] = {}

    # ---- directly-mapped tables -------------------------------------------
    for slug, spec in (
        ("school", schema.TABLE_SCHOOL),
        ("school_district", schema.TABLE_DISTRICT),
        ("district_finance", None),
    ):
        if slug not in tables:
            continue
        table = spec or finance_table()
        src = utils.download(utils.BULK_FILES[slug])
        years = source_years(con, src)
        if only_years:
            years = [y for y in years if y in only_years]
        log.info(f"{slug}: {len(years)} years ({years[0]}-{years[-1]})")
        total = 0
        for y in years:
            t0 = time.time()
            n = utils.clean_wide_table(con, table, src, out, y)
            total += n
            log.info(f"  {slug} {y}: {n:,} rows ({time.time() - t0:.1f}s)")
        counts[slug] = total

    # ---- staff (reshaped from the agency directory) ------------------------
    if "staff" in tables:
        src = utils.download(utils.BULK_FILES["school_district"])
        years = source_years(con, src)
        if only_years:
            years = [y for y in years if y in only_years]
        log.info(f"staff: {len(years)} years ({years[0]}-{years[-1]})")
        total = 0
        for y in years:
            t0 = time.time()
            n = utils.clean_staff(con, src, out, y)
            total += n
            log.info(f"  staff {y}: {n:,} rows ({time.time() - t0:.1f}s)")
        counts["staff"] = total

    # ---- enrollment (one CSV per year, deleted as we go) -------------------
    if "school_enrollment" in tables:
        total = 0
        years = [
            y
            for y in utils.ENROLLMENT_YEARS
            if not only_years or y in only_years
        ]
        for y in years:
            dest = out / "school_enrollment" / f"year={y}" / "data.parquet"
            src = utils.input_dir() / utils.ENROLLMENT_FILE.format(year=y)
            if dest.exists() and not src.exists():
                log.info(f"  enrollment {y}: already converted, skipping")
                continue
            t0 = time.time()
            utils.download(utils.ENROLLMENT_FILE.format(year=y))
            n = utils.clean_enrollment(con, src, out, y)
            total += n
            size_mb = src.stat().st_size / 1e6
            if not args.keep_csv:
                src.unlink()
            log.info(
                f"  enrollment {y}: {n:,} rows from {size_mb:,.0f} MB "
                f"({time.time() - t0:.1f}s)"
            )
        counts["school_enrollment"] = total

    # ---- dictionary --------------------------------------------------------
    if "dicionario" in tables:
        values = ROOT / "architecture" / "dicionario_values.csv"
        counts["dicionario"] = utils.write_dictionary(con, values, out)
        log.info(f"dicionario: {counts['dicionario']:,} rows")

    log.info("=" * 58)
    for slug, n in counts.items():
        log.info(f"{slug:<20} {n:>15,} rows")

    # Merged, not replaced: a partial run (`--tables school_enrollment`) must
    # not erase the counts of the tables it did not touch, since upload.py
    # verifies each table against this file.
    path = utils.data_dir() / "row_counts.json"
    merged = json.loads(path.read_text()) if path.exists() else {}
    merged.update(counts)
    path.write_text(json.dumps(merged, indent=1, sort_keys=True))


if __name__ == "__main__":
    main()
