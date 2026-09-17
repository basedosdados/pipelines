"""Rebuild the dicionario parquet from the harvested chunks.

A thin wrapper. The implementation lives in
``pipelines/datasets/br_mgi_compras_publicas/dicionario.py`` because the weekly
refresh flow rebuilds this table too, and two copies of the code would drift.

    uv run python models/br_mgi_compras_publicas/code/build_dicionario.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipelines.datasets.br_mgi_compras_publicas.dicionario import (  # noqa: E402
    build_dicionario,
)


def data_dir() -> Path:
    """Scratch root holding the harvested chunks and consolidated parquet.

    Returns:
        The path named by ``COMPRAS_DATA_DIR``, or the default under Downloads.
    """
    return Path(
        os.environ.get(
            "COMPRAS_DATA_DIR",
            Path.home() / "Downloads" / "br_mgi_compras_publicas_data",
        )
    )


def main() -> int:
    """Rebuild the dicionario and report how many pairs it holds.

    Returns:
        0 when rows were written, 1 when nothing has been harvested yet.
    """
    root = data_dir()
    rows = build_dicionario(root)
    if not rows:
        print("no chunks harvested yet - nothing to build")
        return 1
    print(f"wrote {rows:,} rows -> {root / 'output' / 'dicionario'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
