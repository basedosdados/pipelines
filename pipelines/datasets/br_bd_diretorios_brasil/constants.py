"""
Constantes de br_bd_diretorios_brasil.
"""

from enum import Enum


class constants(Enum):
    """Constantes de br_bd_diretorios_brasil."""

    # Área de trabalho do pod. `input/` recebe o CSV do Catálogo, `output/` o
    # parquet que sobe para a staging.
    PATH = "/tmp/br_bd_diretorios_brasil/escola/"
