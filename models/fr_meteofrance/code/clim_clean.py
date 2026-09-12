"""Clean the *Données climatologiques de base* archives into partitioned parquet.

    uv run python models/fr_meteofrance/code/clim_clean.py [--only quot|mens|poste]
                                                           [--latest-only]

A thin CLI. The transform itself lives in
``pipelines/datasets/fr_meteofrance/clim_utils.py`` so the one-shot bootstrap and
the recurring flow share one copy; see that module for the layout of the output
and why staging is one parquet per (département, period) rather than
hive-partitioned by year.

``poste`` is always rebuilt from EVERY source file, never from whichever series
or slice this run happened to touch: a station can appear in the monthly files
and not the daily ones, in a historical slice and not the latest one, or the
reverse. Building it as a by-product of one pass silently dropped stations.
"""

import argparse

from pipelines.datasets.fr_meteofrance import clim_schema as cs
from pipelines.datasets.fr_meteofrance.clim_utils import (
    INPUT,
    OUTPUT,
    build_poste,
    clean_mens,
    clean_quot,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--only", choices=["quot", "mens", "poste"])
    parser.add_argument(
        "--latest-only",
        action="store_true",
        help="rebuild only the latest-<years> slice (never applies to poste)",
    )
    args = parser.parse_args()
    descriptors = cs.descriptors()
    sink: dict = {}
    print(f"input {INPUT}\noutput {OUTPUT}")
    if args.only in (None, "mens"):
        n = clean_mens(descriptors, sink, only_latest=args.latest_only)
        print("mensuelle:", f"{n:,} rows")
    if args.only in (None, "quot"):
        n = clean_quot(descriptors, sink, only_latest=args.latest_only)
        print("quotidienne:", f"{n:,} rows")
    if args.only in (None, "poste"):
        print("poste:", f"{build_poste():,} rows")


if __name__ == "__main__":
    main()
