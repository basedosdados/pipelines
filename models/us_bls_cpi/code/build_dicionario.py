#!/usr/bin/env python3
"""Bootstrap: build the `dicionario` parquet from the CPI dimension files in
../input into ../output.

Logic lives in `pipelines.datasets.us_bls_cpi.utils.build_dicionario` (shared
with the recurring pipeline).

Usage:
    uv run python models/us_bls_cpi/code/build_dicionario.py
"""

import logging
from pathlib import Path

from pipelines.datasets.us_bls_cpi.utils import build_dicionario

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

ROOT = Path(__file__).resolve().parents[1]

if __name__ == "__main__":
    build_dicionario(ROOT / "input", ROOT / "output")
