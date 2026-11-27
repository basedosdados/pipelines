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


def main(tabelas: list[str]) -> None:
    """baixa sempre o arquivo atual e limpa cada tabela."""
    for tabela in tabelas:
        item = buscar_item_central(constants.NOME_CENTRAL.value[tabela])
        csv = baixar_csv(item["url_csv"], PASTA / "input" / f"{tabela}.csv")
        limpar(csv, PASTA / "output" / tabela, tabela)


if __name__ == "__main__":
    main(sys.argv[1:] or list(constants.NOME_CENTRAL.value))
