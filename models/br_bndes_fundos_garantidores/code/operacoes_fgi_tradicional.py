"""carga inicial da operacoes_fgi_tradicional; reusa as funções do pipeline."""

import os
from pathlib import Path

from pipelines.datasets.br_bndes_fundos_garantidores.constants import (
    constants,
)
from pipelines.datasets.br_bndes_fundos_garantidores.utils import (
    baixar_csv,
    buscar_item_central,
    limpar,
)

TABELA = "operacoes_fgi_tradicional"
PASTA = Path(
    os.environ.get(
        "BNDES_FGI_DATA",
        Path.home() / "Downloads" / "br_bndes_fundos_garantidores_data",
    )
)

if __name__ == "__main__":
    item = buscar_item_central(constants.NOME_CENTRAL.value[TABELA])
    csv = PASTA / "input" / f"{TABELA}_{item['posicao']:%Y-%m}.csv"
    if not csv.exists():
        baixar_csv(item["url_csv"], csv)
    limpar(csv, PASTA / "output" / TABELA)
