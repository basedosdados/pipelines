"""Bootstrap: build the us_bls_oes `dicionario` parquet from the cleaned output.

Logic lives in `pipelines.datasets.us_bls_oes.utils.build_dicionario` (shared
with the recurring pipeline). Run it after clean_data.py, since the dictionary
keys are read back from the written partitions.

Run: uv run python models/us_bls_oes/code/build_dicionario.py
"""

import logging
import os
from pathlib import Path

from pipelines.datasets.us_bls_oes.utils import build_dicionario

ROOT = Path(
    os.environ.get("OES_DATA_DIR", Path.home() / "Downloads/us_bls_oes_data")
)
OUTPUT_DIR = Path(os.environ.get("OES_OUTPUT_DIR", ROOT / "output"))

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    build_dicionario(OUTPUT_DIR)
