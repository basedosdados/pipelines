"""One-shot bootstrap: build world_wb_wdi tables from WDI_CSV.zip.

Reuses the exact transform the recurring pipeline runs
(``pipelines.datasets.world_wb_wdi.utils``) so the two cannot drift. Downloads
and extracts the World Bank bulk archive, then writes all six tables as
all-STRING partitioned Parquet.

Scratch data lives outside the repo (never under Dropbox/git):
``~/Downloads/world_wb_wdi_data/`` by default, overridable via ``WDI_DATA_DIR``.

Run:  python models/world_wb_wdi/code/clean.py
"""

import os
from pathlib import Path

from pipelines.datasets.world_wb_wdi.utils import clean_all, download_source

DATA_DIR = Path(
    os.environ.get(
        "WDI_DATA_DIR", Path.home() / "Downloads" / "world_wb_wdi_data"
    )
)
INPUT = DATA_DIR / "input"
OUTPUT = DATA_DIR / "output"


def main():
    download_source(INPUT)
    result = clean_all(INPUT, OUTPUT)
    print("\nROW COUNTS:", result["counts"])
    print("MAX YEAR:", result["max_year"])
    print("OUTPUT:", OUTPUT)


if __name__ == "__main__":
    main()
