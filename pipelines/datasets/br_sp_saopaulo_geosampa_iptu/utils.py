"""
Utils for br_sp_saopaulo_geosampa_iptu.
"""

import datetime

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
