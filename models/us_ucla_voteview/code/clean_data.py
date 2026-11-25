"""Download and clean Voteview into partitioned all-STRING parquet.

Usage:
    uv run --no-sync python models/us_ucla_voteview/code/clean_data.py [--skip-download]

Writes ``$US_UCLA_VOTEVIEW_DATA_DIR/{input,output}`` (default
``~/Library/Caches/us_ucla_voteview_data``). The transform itself lives in
``pipelines/datasets/us_ucla_voteview/utils.py`` and is shared with the
recurring pipeline.
"""

import logging
import sys

from pipelines.datasets.us_ucla_voteview.constants import constants
from pipelines.datasets.us_ucla_voteview.utils import clean_all, download_all

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")


def main():
    root = constants.DATA_DIR.value
    if "--skip-download" not in sys.argv:
        download_all(root / "input")
    result = clean_all(root / "input", root / "output")
    for table, n in result["rows"].items():
        print(f"{table}: {n:,} rows")
    print(f"latest roll call: {result['max_date']}")


if __name__ == "__main__":
    main()
