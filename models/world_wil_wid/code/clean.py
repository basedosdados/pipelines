"""One-shot bootstrap: download and clean the full WID.world dataset.

The transform itself lives in ``pipelines/datasets/world_wil_wid/utils.py`` and
is imported here rather than duplicated, so the recurring Prefect pipeline and
this bootstrap can never drift apart.

Scratch data goes to ``~/Downloads/world_wil_wid_data`` by default -- never into
the repo or anywhere under Dropbox, which would trigger a multi-GB sync. Set
``WID_DATA_DIR`` to override.

Usage::

    uv run python models/world_wil_wid/code/clean.py            # download + clean
    uv run python models/world_wil_wid/code/clean.py --no-download   # reuse the zip
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from pipelines.datasets.world_wil_wid.utils import (
    clean_all,
    download_bulk,
    source_last_modified,
)

DEFAULT_ROOT = Path.home() / "Downloads" / "world_wil_wid_data"


def main() -> None:
    """Download (optionally) and clean, reporting a row count per table."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--no-download",
        action="store_true",
        help="Reuse an already downloaded wid_all_data.zip in <root>/input.",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(os.environ.get("WID_DATA_DIR", DEFAULT_ROOT)),
        help="Scratch root holding input/ and output/.",
    )
    args = parser.parse_args()

    input_dir = args.root / "input"
    output_dir = args.root / "output"

    print(f"source Last-Modified: {source_last_modified()}")
    if not args.no_download:
        download_bulk(input_dir)

    counts = clean_all(input_dir, output_dir)
    print("\n=== rows written ===")
    for table, rows in counts.items():
        print(f"{table:<12} {rows:>12,}")


if __name__ == "__main__":
    main()
