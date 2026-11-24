"""
carga inicial do iptu: baixa e trata os anos pedidos em parquet particionado.

o tratamento fica em `pipelines.datasets.br_sp_saopaulo_geosampa_iptu.utils`,
compartilhado com o pipeline recorrente.

uso:
    uv run models/br_sp_saopaulo_geosampa_iptu/code/iptu.py [ano ...]
"""

import os
import sys
from pathlib import Path

from pipelines.datasets.br_sp_saopaulo_geosampa_iptu.utils import (
    baixar_ano,
    tratar_ano,
    ultimo_ano_publicado,
)

PRIMEIRO_ANO = 1995
# fora do repositório, para não versionar dados
BASE = Path(
    os.environ.get(
        "IPTU_DATA_DIR",
        Path.home() / "Downloads" / "br_sp_saopaulo_geosampa_iptu_data",
    )
)


def main() -> None:
    """trata os anos de `argv`, ou todos os publicados se nenhum for passado."""
    anos = [int(a) for a in sys.argv[1:]] or list(
        range(PRIMEIRO_ANO, ultimo_ano_publicado() + 1)
    )
    for ano in anos:
        csv_path = baixar_ano(ano, BASE / "input")
        tratar_ano(csv_path, BASE / "output", ano)
        csv_path.unlink()
        print(f"{ano}: ok")


if __name__ == "__main__":
    main()
