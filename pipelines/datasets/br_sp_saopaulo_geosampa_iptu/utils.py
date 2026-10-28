"""
Utils for br_sp_saopaulo_geosampa_iptu.
"""

import datetime
import zipfile
from pathlib import Path

import requests

from pipelines.datasets.br_sp_saopaulo_geosampa_iptu.constants import (
    HEADERS,
    MAX_ANOS_BUSCA,
    URL_TEMPLATE,
)


def ano_existe(ano: int) -> bool:
    """indica se o zip do ano está publicado."""
    url = URL_TEMPLATE.format(ano=ano)
    with requests.get(url, headers=HEADERS, stream=True, timeout=60) as r:
        r.raise_for_status()
        # pedaços podem ter 1 byte
        inicio = b""
        for pedaco in r.iter_content(chunk_size=1024):
            inicio += pedaco
            if len(inicio) >= 2:
                break
    # ano inexistente também responde 200, com html
    return inicio.startswith(b"PK")


def ultimo_ano_publicado(hoje: datetime.date | None = None) -> int:
    """devolve o ano mais recente publicado."""
    hoje = hoje or datetime.date.today()
    primeiro = hoje.year + 1
    for ano in range(primeiro, primeiro - MAX_ANOS_BUSCA, -1):
        if ano_existe(ano):
            return ano
    raise ValueError(
        f"Nenhum IPTU publicado entre {primeiro - MAX_ANOS_BUSCA + 1} e "
        f"{primeiro}; a URL do GeoSampa pode ter mudado."
    )


def baixar_ano(ano: int, destino: Path) -> Path:
    """baixa o zip do ano, extrai o csv e devolve o caminho dele."""
    destino.mkdir(parents=True, exist_ok=True)
    zip_path = destino / f"IPTU_{ano}.zip"
    url = URL_TEMPLATE.format(ano=ano)
    with requests.get(url, headers=HEADERS, stream=True, timeout=600) as r:
        r.raise_for_status()
        with open(zip_path, "wb") as f:
            for pedaco in r.iter_content(chunk_size=1024 * 1024):
                f.write(pedaco)

    try:
        if not zipfile.is_zipfile(zip_path):
            raise ValueError(
                f"IPTU {ano} não é um ZIP; o ano pode não existir."
            )
        with zipfile.ZipFile(zip_path) as z:
            csvs = [n for n in z.namelist() if n.lower().endswith(".csv")]
            if len(csvs) != 1:
                raise ValueError(
                    f"esperado 1 CSV no ZIP de {ano}, achados: {csvs}"
                )
            csv_path = Path(z.extract(csvs[0], destino))
    finally:
        zip_path.unlink()
    return csv_path
