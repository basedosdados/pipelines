"""Build the us_census_lodes `dicionario` table.

The build itself lives in ``pipelines/datasets/us_census_lodes/utils.py`` and is
shared with the recurring flow, which rebuilds it on every run. This script only
drives it for the one-shot onboarding.

    uv run python models/us_census_lodes/code/gen_dicionario.py

`job_type` is the only codified column in this dataset. Every other
coded-looking column (block, tract, county and state identifiers) resolves
through a directory, not through this dictionary.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.us_census_lodes.constants import OUTPUT
from pipelines.datasets.us_census_lodes.utils import (
    build_dicionario,
)


def main() -> None:
    rows = build_dicionario(OUTPUT)
    print(
        f"dicionario: {rows} rows -> {OUTPUT / 'dicionario' / 'data.parquet'}"
    )


if __name__ == "__main__":
    main()
