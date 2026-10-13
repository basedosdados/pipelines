"""
Detecção de edições e download de br_inep_censo_educacao_superior.

O módulo não importa Prefect. As funções são chamadas pelas tasks de `tasks.py`
e também podem ser executadas diretamente.

O acesso ao `download.inep.gov.br` reaproveita o `br_inep_enem`, que já trata a
cadeia de certificados incompleta do servidor, as conexões derrubadas e a
retomada de downloads interrompidos.
"""

import datetime
import re
from email.utils import parsedate_to_datetime
from pathlib import Path

from pipelines.datasets.br_inep_censo_educacao_superior.constants import (
    constants,
)
from pipelines.datasets.br_inep_enem.utils import (
    HEADERS,
    build_session,
    download_zip,
)


def get_source_max_year() -> int:
    """Lê na página do Inep o ano mais recente publicado.

    O valor é a edição do Censo, não a data da consulta.

    Returns:
        O ano mais recente.

    Raises:
        ValueError: Se a página não listar nenhum zip de microdados.
    """
    response = build_session().get(
        constants.SOURCE_LINK.value,
        headers=HEADERS,
        verify=False,
        timeout=120,
    )
    response.raise_for_status()

    anos = re.findall(
        r"microdados_censo_da_educacao_superior_(\d{4})\.zip", response.text
    )
    if not anos:
        raise ValueError(
            "nenhum microdados_censo_da_educacao_superior_<ano>.zip em "
            f"{constants.SOURCE_LINK.value}"
        )
    return max(int(ano) for ano in anos)


def last_modified(ano: int) -> datetime.datetime:
    """Lê a data em que o Inep publicou o zip do ano pela última vez.

    Só o cabeçalho da resposta é lido; o corpo não é baixado. A requisição é um
    GET com `stream=True`, e não um HEAD, porque a repetição de `build_session`
    cobre apenas GET e o servidor do Inep derruba conexões com frequência.

    A data vem do cabeçalho `Last-Modified` do servidor. O texto "Atualizado em"
    da página não serve para isso: ele não acompanha as republicações.

    Args:
        ano: Edição do Censo.

    Returns:
        A data de modificação do zip, com fuso horário (UTC).

    Raises:
        requests.HTTPError: Se o zip do ano não existir.
        ValueError: Se o servidor não informar `Last-Modified`.
    """
    url = constants.DOWNLOAD_LINK.value.format(ano=ano)
    with build_session().get(
        url, headers=HEADERS, verify=False, stream=True, timeout=60
    ) as response:
        response.raise_for_status()
        cabecalho = response.headers.get("Last-Modified")

    if not cabecalho:
        raise ValueError(f"{url}: resposta sem Last-Modified")
    return parsedate_to_datetime(cabecalho)


def download_censo(ano: int, input_dir: Path) -> Path:
    """Baixa o zip do Censo da Educação Superior do ano.

    Se o zip já estiver em `input_dir`, o download é retomado de onde parou; se
    estiver completo, nada é baixado.

    Args:
        ano: Edição do Censo.
        input_dir: Diretório onde gravar o zip. Precisa existir.

    Returns:
        O caminho do zip.
    """
    url = constants.DOWNLOAD_LINK.value.format(ano=ano)
    zip_path = input_dir / url.split("/")[-1]
    download_zip(url, zip_path)
    return zip_path
