"""
Build partitioned parquet for the br_senado_dados_abertos_administrativos
onboarding.

  uv run python models/br_senado_dados_abertos_administrativos/code/run_onboarding.py --sample
  uv run python models/br_senado_dados_abertos_administrativos/code/run_onboarding.py --full

All-STRING parquet under ``$SENADO_ADM_DATA/output/<table>/``, defaulting to
``~/Downloads/br_senado_dados_abertos_administrativos_data`` — never inside the
repo or Dropbox, which would sync gigabytes and risk committing data.

Reuses the pipeline's cleaning transform
(``pipelines.datasets.br_senado_dados_abertos_administrativos``), the single
source of truth for the extract, shared with the recurring pipeline.
``upload.py`` then uploads the output to BigQuery.
"""

from __future__ import annotations

import argparse
import datetime as dt
import glob
import os
import time

import pyarrow.parquet as pq

from pipelines.datasets.br_senado_dados_abertos_administrativos.utils import (
    ALL_TABLES,
    TABLES,
    clean_all,
)

DATA_DIR = os.environ.get(
    "SENADO_ADM_DATA",
    os.path.expanduser(
        "~/Downloads/br_senado_dados_abertos_administrativos_data"
    ),
)
OUTPUT = os.path.join(DATA_DIR, "output")


def rows_in(out_dir: str) -> int:
    files = glob.glob(os.path.join(out_dir, "**", "*.parquet"), recursive=True)
    return sum(pq.read_metadata(f).num_rows for f in files)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--sample",
        action="store_true",
        help="recent slice, no contratação sub-resource crawl; fast",
    )
    ap.add_argument("--full", action="store_true", help="full history")
    ap.add_argument(
        "--no-sub-resources",
        action="store_true",
        help="skip the contratação fan-out even on a full run",
    )
    args = ap.parse_args()
    if not (args.sample or args.full):
        ap.error("pass --sample or --full")

    # Sample: the current year only for the four time series, and no per-entity
    # crawl — enough to exercise every builder without the 27k-request fan-out.
    years = [dt.date.today().year] if args.sample else None
    sub_resources = args.full and not args.no_sub_resources

    os.makedirs(OUTPUT, exist_ok=True)
    started = time.time()
    print(f"writing to {OUTPUT}")
    print(f"  years={'full history' if years is None else years}")
    print(f"  contratação sub-resources={sub_resources}")

    result = clean_all(OUTPUT, years=years, sub_resources=sub_resources)

    print("\n=== SUMMARY ===")
    total = 0
    for table in ALL_TABLES:
        if table not in result:
            print(f"  {table:34} {'skipped':>10}")
            continue
        count = rows_in(result[table])
        total += count
        part = TABLES[table]["partition"] or "-"
        print(f"  {table:34} {count:>10,} rows   partition={part}")
    print(f"  {'TOTAL':34} {total:>10,} rows")
    print(f"\nelapsed {time.time() - started:,.0f}s")


if __name__ == "__main__":
    main()
