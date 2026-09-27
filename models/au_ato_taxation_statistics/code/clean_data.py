"""One-shot onboarding bootstrap for au_ato_taxation_statistics.

Reuses the pure transform in
``pipelines/datasets/au_ato_taxation_statistics/utils.py`` so the
recurring Prefect flow and this bootstrap can never drift apart.

Usage::

    uv run python models/au_ato_taxation_statistics/code/clean_data.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO))

from pipelines.datasets.au_ato_taxation_statistics import utils  # noqa: E402

DATA_DIR = Path(
    os.environ.get(
        "ATO_TAXSTATS_DATA",
        Path.home() / "Downloads" / "au_ato_taxation_statistics_data",
    )
)


def main() -> None:
    """Clean every workbook in ``input/`` into partitioned parquet."""
    input_dir = DATA_DIR / "input"
    output_dir = DATA_DIR / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    counts = utils.clean_all(input_dir, output_dir)
    total = 0
    for table, rows in sorted(counts.items()):
        print(f"{table:28s} {rows:>10,} rows")
        total += rows
    print(f"{'TOTAL':28s} {total:>10,} rows")


if __name__ == "__main__":
    main()
