"""
Tasks for br_sp_saopaulo_geosampa_iptu.
"""

import datetime

from prefect import task

from pipelines.constants import constants as repo_constants
from pipelines.datasets.br_sp_saopaulo_geosampa_iptu.utils import (
    ultimo_ano_publicado,
)
from pipelines.utils.stage_dispatch import SourceInspection


@task(
    retries=repo_constants.TASK_MAX_RETRIES.value,
    retry_delay_seconds=repo_constants.TASK_RETRY_DELAY.value,
)
def get_ultimo_ano_publicado() -> int:
    """Wrapper Prefect de `ultimo_ano_publicado`, com retries para falhas
    de rede."""
    return ultimo_ano_publicado()


def iptu_get_latest_update() -> SourceInspection:
    """check_update da tabela `iptu`: devolve 1º de janeiro do ano mais
    recente publicado, comparado contra a cobertura da tabela."""
    ano = get_ultimo_ano_publicado()
    return SourceInspection(reference_date=datetime.date(ano, 1, 1))
