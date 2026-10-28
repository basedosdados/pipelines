"""
Utils for br_sp_saopaulo_geosampa_iptu.

Funções puras (sem Prefect), reaproveitáveis fora do flow.
"""

import datetime

import requests

from pipelines.datasets.br_sp_saopaulo_geosampa_iptu.constants import (
    HEADERS,
    MAX_ANOS_BUSCA,
    URL_TEMPLATE,
)


def ano_existe(ano: int) -> bool:
    """Indica se o ZIP do IPTU do ano está publicado no GeoSampa.

    Um ano inexistente também responde HTTP 200, com uma página HTML no
    lugar do arquivo, então o status não basta: confere se a resposta
    começa com a assinatura de ZIP (`PK`). Só lê o começo da resposta —
    o arquivo não é baixado.

    Args:
        ano: ano de exercício.

    Returns:
        `True` se o ZIP existe, `False` caso contrário.

    Raises:
        requests.HTTPError: se o servidor responder com erro (4xx/5xx).
    """
    url = URL_TEMPLATE.format(ano=ano)
    with requests.get(url, headers=HEADERS, stream=True, timeout=60) as r:
        r.raise_for_status()
        # O corpo vem em gzip e um pedaço pode ter só 1 byte: junta até 2.
        inicio = b""
        for pedaco in r.iter_content(chunk_size=1024):
            inicio += pedaco
            if len(inicio) >= 2:
                break
    return inicio.startswith(b"PK")


def ultimo_ano_publicado(hoje: datetime.date | None = None) -> int:
    """Acha o ano mais recente do IPTU publicado no GeoSampa.

    Começa no ano seguinte ao atual e desce até achar um ano publicado.

    Args:
        hoje: data de referência; `None` usa a data atual.

    Returns:
        O ano mais recente publicado.

    Raises:
        ValueError: se nenhum ano for achado em `MAX_ANOS_BUSCA` tentativas,
            o que indica mudança na fonte (ex.: URL nova).
    """
    hoje = hoje or datetime.date.today()
    primeiro = hoje.year + 1
    for ano in range(primeiro, primeiro - MAX_ANOS_BUSCA, -1):
        if ano_existe(ano):
            return ano
    raise ValueError(
        f"Nenhum IPTU publicado entre {primeiro - MAX_ANOS_BUSCA + 1} e "
        f"{primeiro}; a URL do GeoSampa pode ter mudado."
    )
