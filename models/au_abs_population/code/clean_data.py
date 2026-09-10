"""One-shot onboarding bootstrap for au_abs_population.

The download and cleaning transform lives in
``pipelines/datasets/au_abs_population/utils.py`` so that this bootstrap and
the recurring Prefect pipeline share exactly one implementation. This script
only wires it to a scratch directory.

Scratch data goes under ``~/Downloads/au_abs_population_data`` by default --
never inside the repo or Dropbox -- and is deleted once the onboarding is
verified. Override with AU_ABS_POPULATION_DATA.

Usage:
    python clean_data.py [--download]
"""

import argparse
import os
import sys

REPO = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..")
)
sys.path.insert(0, REPO)

from pipelines.datasets.au_abs_population.utils import (  # noqa: E402
    clean_all,
    download_all,
)

DATA = os.environ.get(
    "AU_ABS_POPULATION_DATA",
    os.path.expanduser("~/Downloads/au_abs_population_data"),
)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--download",
        action="store_true",
        help="fetch the source workbooks first (skipped if already present)",
    )
    args = ap.parse_args()

    input_dir = os.path.join(DATA, "input")
    output_dir = os.path.join(DATA, "output")
    if args.download:
        slugs = download_all(input_dir)
        print(f"release slugs: {slugs}")
    result = clean_all(input_dir, output_dir)
    print(f"\noutput: {output_dir}")
    print(
        f"max_year_quarter={result['max_year_quarter']} max_year={result['max_year']}"
    )


if __name__ == "__main__":
    main()
