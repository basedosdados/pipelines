"""
Tasks for br_sp_saopaulo_geosampa_iptu.
"""

import datetime
import shutil

from prefect import task

from pipelines.constants import constants as repo_constants
from pipelines.datasets.br_sp_saopaulo_geosampa_iptu.constants import (
    COVERAGE,
    PATH_INPUT,
    PATH_OUTPUT,
)
from pipelines.datasets.br_sp_saopaulo_geosampa_iptu.utils import (
    baixar_ano,
    tratar_ano,
    ultimo_ano_publicado,
)
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection


@task(
    retries=repo_constants.TASK_MAX_RETRIES.value,
    retry_delay_seconds=repo_constants.TASK_RETRY_DELAY.value,
)
def get_ultimo_ano_publicado() -> int:
    """wrapper prefect de `ultimo_ano_publicado`."""
    return ultimo_ano_publicado()


def iptu_get_latest_update() -> SourceInspection:
    """check_update da tabela `iptu`."""
    ano = get_ultimo_ano_publicado()
    return SourceInspection(reference_date=datetime.date(ano, 1, 1))


def iptu_extract_load_data(download_params: dict) -> ExtractAndLoad:
    """extract_and_load da tabela `iptu`: baixa e trata o ano encontrado."""
    ano = datetime.date.fromisoformat(download_params["reference_date"]).year
    # só a partição desta execução vai para o upload
    shutil.rmtree(PATH_OUTPUT, ignore_errors=True)
    csv_path = baixar_ano(ano, PATH_INPUT)
    tratar_ano(csv_path, PATH_OUTPUT, ano)
    csv_path.unlink()
    return ExtractAndLoad(
        coverage=COVERAGE.model_dump(),
        data_path=str(PATH_OUTPUT),
        source_format="parquet",
    )
