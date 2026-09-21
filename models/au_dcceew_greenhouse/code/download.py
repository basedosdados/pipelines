"""One-shot onboarding download for au_dcceew_greenhouse.

Thin CLI wrapper. The canonical download/clean transform lives once in
``pipelines.datasets.au_dcceew_greenhouse.utils`` and is imported here so the
onboarding bootstrap and the recurring pipeline never diverge.

Usage:
    uv run python models/au_dcceew_greenhouse/code/download.py
"""

import os
from pathlib import Path

from pipelines.datasets.au_dcceew_greenhouse.utils import download_all

OUTPUT_ROOT = Path(
    os.environ.get(
        "AU_DCCEEW_GREENHOUSE_DATA",
        Path.home() / "Downloads" / "au_dcceew_greenhouse_data",
    )
)


if __name__ == "__main__":
    print(f"Downloading to {OUTPUT_ROOT / 'input'}", flush=True)
    (OUTPUT_ROOT / "input").mkdir(parents=True, exist_ok=True)
    download_all(OUTPUT_ROOT / "input")
    print("done", flush=True)
