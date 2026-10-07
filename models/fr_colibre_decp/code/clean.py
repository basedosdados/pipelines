"""One-shot onboarding bootstrap: download decp.parquet and build the three tables.

Usage (from the repo root):
    python -m models.fr_colibre_decp.code.clean [--skip-download]

Data lands under constants.DATA_DIR (override with FR_COLIBRE_DECP_DATA_DIR):
``input/decp.parquet`` and ``output/<table>/ano=YYYY/data_0.parquet``. The
transform itself lives in pipelines/datasets/fr_colibre_decp/utils.py and is
shared with the recurring pipeline.
"""

import sys

from pipelines.datasets.fr_colibre_decp import utils
from pipelines.datasets.fr_colibre_decp.constants import constants


def main() -> None:
    data_dir = constants.DATA_DIR.value
    source = data_dir / "input" / "decp.parquet"
    if "--skip-download" not in sys.argv or not source.exists():
        print(f"source last modified: {utils.source_last_modified()}")
        source = utils.download_decp(data_dir / "input")
    counts = utils.clean_decp(source, data_dir / "output")
    for table, n in counts.items():
        print(f"{table}: {n:,} rows")
    print(f"max month: {utils.source_max_date(data_dir / 'output')}")


if __name__ == "__main__":
    main()
