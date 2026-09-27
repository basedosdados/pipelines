"""Download and clean au_aec_elections into partitioned parquet.

Thin bootstrap over ``pipelines.datasets.au_aec_elections.utils`` so the one-shot
onboarding and any future recurring pipeline share one transform.

Usage:
    uv run python models/au_aec_elections/code/clean.py [--skip-download]

Raw downloads and cleaned parquet go to $AEC_DATA (default
~/Downloads/au_aec_elections_data), never inside the repo or Dropbox.
"""

from __future__ import annotations

import sys

from pipelines.datasets.au_aec_elections import utils
from pipelines.datasets.au_aec_elections.constants import data_root


def main() -> None:
    root = data_root()
    if "--skip-download" not in sys.argv[1:]:
        print(f"[download] -> {root / 'input'}")
        utils.download_all(root)
    print(f"[clean] -> {root / 'output'}")
    counts = utils.clean_all(root)
    total = 0
    for table, n in counts.items():
        print(f"  {table:46s} {n:>10,}")
        total += n
    print(f"  {'TOTAL':46s} {total:>10,}")


if __name__ == "__main__":
    main()
