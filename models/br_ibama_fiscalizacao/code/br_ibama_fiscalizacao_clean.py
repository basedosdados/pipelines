"""Clean IBAMA enforcement records (autos de infração and termos de embargo).

One-shot entry point for the onboarding. The transform itself lives in
``pipelines/datasets/br_ibama_fiscalizacao/utils.py`` and is shared with the
recurring pipeline, so the two cannot drift apart.

Usage, from the repo root::

    python -m models.br_ibama_fiscalizacao.code.br_ibama_fiscalizacao_clean
    python -m models.br_ibama_fiscalizacao.code.br_ibama_fiscalizacao_clean --download

``IBAMA_DATA_DIR`` overrides the scratch directory (see ``constants.py``).
"""

from __future__ import annotations

import logging
import sys

from models.br_ibama_fiscalizacao.code.constants import INPUT_DIR, OUTPUT_DIR
from pipelines.datasets.br_ibama_fiscalizacao.utils import (
    clean_all,
    download_all,
)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if "--download" in sys.argv[1:]:
        download_all(INPUT_DIR)
    result = clean_all(INPUT_DIR, OUTPUT_DIR)
    print(f"\ndata_extracao: {result['data_extracao']}")
    for t, n in result["rows"].items():
        print(f"{t}: {n:,} rows")
    print("\ndiagnostics:")
    for k, v in sorted(result["diagnostics"].items()):
        print(f"  {k:<22} {v:,}")


if __name__ == "__main__":
    main()
