"""carga inicial dos fundos (todos ou os passados como argumento); reusa as funções do pipeline."""

import os
import sys
from pathlib import Path

from pipelines.datasets.br_bndes_fundos_garantidores.constants import (
    constants,
)
from pipelines.datasets.br_bndes_fundos_garantidores.utils import (
    baixar_csv,
    buscar_item_central,
    limpar,
)

PASTA = Path(
    os.environ.get(
        "BNDES_FGI_DATA",
        Path.home() / "Downloads" / "br_bndes_fundos_garantidores_data",
    )
)

if __name__ == "__main__":
    for tabela in sys.argv[1:] or list(constants.NOME_CENTRAL.value):
        item = buscar_item_central(constants.NOME_CENTRAL.value[tabela])
        csv = PASTA / "input" / f"{tabela}_{item['posicao']:%Y-%m}.csv"
        if not csv.exists():
            baixar_csv(item["url_csv"], csv)
        limpar(csv, PASTA / "output" / tabela, tabela)
