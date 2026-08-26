"""Build the br_pncp dicionario table for the one-shot onboarding.

Front end for ``pipelines.datasets.br_pncp.utils.build_dicionario``. Most PNCP
coded fields ship their label alongside the code, so the dictionary is derived
from the cleaned data and cannot drift from what the tables contain; only
id_esfera, id_poder and tipo_pessoa_fornecedor are hard-coded, because the API
never labels them.

Usage:
    uv run python models/br_pncp/code/build_dicionario.py
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.br_pncp.utils import build_dicionario

DATA_DIR = Path(
    os.environ.get("PNCP_DATA_DIR", Path.home() / "Downloads" / "br_pncp_data")
)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--output-dir", type=Path, default=DATA_DIR / "output")
    args = ap.parse_args()
    build_dicionario(args.output_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
