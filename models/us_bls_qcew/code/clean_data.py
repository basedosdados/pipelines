#!/usr/bin/env python3
"""Bootstrap: download + clean QCEW singlefiles into partitioned parquet.

The transform lives in ``pipelines.datasets.us_bls_qcew.utils`` so the one-shot
onboarding and the (future) recurring Prefect pipeline share one implementation.

Usage:
    # representative subset (dev verification checkpoint)
    uv run python models/us_bls_qcew/code/clean_data.py --subset
    # full history
    uv run python models/us_bls_qcew/code/clean_data.py --full
    # explicit years
    uv run python models/us_bls_qcew/code/clean_data.py --naics 2024 2025 --sic 2000
"""

import argparse
import logging
from pathlib import Path

from pipelines.datasets.us_bls_qcew.constants import constants
from pipelines.datasets.us_bls_qcew.utils import clean_all

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

ROOT = Path(__file__).resolve().parents[1]
INPUT = ROOT / "input"
OUTPUT = ROOT / "output"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--subset", action="store_true", help="use the dev checkpoint subset"
    )
    ap.add_argument(
        "--full", action="store_true", help="use the full coverage"
    )
    ap.add_argument(
        "--naics", nargs="*", type=int, help="explicit NAICS years"
    )
    ap.add_argument("--sic", nargs="*", type=int, help="explicit SIC years")
    ap.add_argument(
        "--workers",
        type=int,
        default=1,
        help="concurrent downloads prefetched ahead of the serial cleaner "
        "(1 = sequential; cleaning is never parallelized)",
    )
    args = ap.parse_args()

    if args.full:
        years = {
            "naics": constants.NAICS_YEARS.value,
            "sic": constants.SIC_YEARS.value,
        }
    elif args.naics is not None or args.sic is not None:
        years = {"naics": args.naics or [], "sic": args.sic or []}
    else:  # default: subset
        years = dict(constants.SUBSET_YEARS.value)

    logging.info(
        f"cleaning years: naics={years['naics']} sic={years['sic']} "
        f"(download workers={args.workers})"
    )
    # On a full-history run, prune each multi-GB CSV once cleaned so input never
    # accumulates (dozens of files) on disk / in Dropbox.
    written = clean_all(
        years,
        INPUT,
        OUTPUT,
        prune_input=args.full,
        download_workers=args.workers,
    )
    logging.info(f"wrote {len(written)} tables into {OUTPUT}")


if __name__ == "__main__":
    main()
