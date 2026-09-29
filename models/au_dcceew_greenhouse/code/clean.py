"""One-shot onboarding clean for au_dcceew_greenhouse.

Thin CLI wrapper over ``pipelines.datasets.au_dcceew_greenhouse.utils.clean_all``
(the canonical transform, shared with the recurring pipeline).

Usage:
    uv run python models/au_dcceew_greenhouse/code/clean.py
"""

import os
from pathlib import Path

from pipelines.datasets.au_dcceew_greenhouse.utils import clean_all

OUTPUT_ROOT = Path(
    os.environ.get(
        "AU_DCCEEW_GREENHOUSE_DATA",
        Path.home() / "Downloads" / "au_dcceew_greenhouse_data",
    )
)


if __name__ == "__main__":
    clean_all(OUTPUT_ROOT / "input", OUTPUT_ROOT / "output")
