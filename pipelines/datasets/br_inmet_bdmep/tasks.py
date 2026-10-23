"""
Tasks Prefect de br_inmet_bdmep.
"""

from datetime import date

from prefect import task

from pipelines.constants import constants as repo_constants
from pipelines.datasets.br_inmet_bdmep.constants import (
    COVERAGE,
    ConstantsMicrodados,
)
from pipelines.datasets.br_inmet_bdmep.utils import (
    build_year_csv,
    download_year_zip,
    get_max_date,
)
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection


@task(
    retries=repo_constants.TASK_MAX_RETRIES.value,
    retry_delay_seconds=repo_constants.TASK_RETRY_DELAY.value,
)
def download_source_zip() -> int:
    """
    Baixa e descompacta o ZIP do ano corrente do INMET.

    Returns:
        int: ano do ZIP baixado.
    """
    return download_year_zip(
        year=date.today().year,
        input_dir=ConstantsMicrodados.PATH_INPUT.value,
    )


@task
def get_source_max_date() -> date:
    """
    Lê a data mais recente da fonte nos nomes dos CSVs baixados.

    Returns:
        date: data final mais recente entre as estações.
    """
    return get_max_date(ConstantsMicrodados.PATH_INPUT.value)


@task
def build_year_file(year: int) -> str:
    """
    Grava o CSV do ano particionado por `ano=`.

    Args:
        year (int): ano do ZIP baixado.

    Returns:
        str: raiz das partições, para o upload.
    """
    build_year_csv(
        input_dir=ConstantsMicrodados.PATH_INPUT.value,
        output_dir=ConstantsMicrodados.PATH_OUTPUT.value,
        year=year,
    )
    return str(ConstantsMicrodados.PATH_OUTPUT.value)


def microdados_get_latest_update() -> SourceInspection:
    """Baixa o ZIP do ano e devolve a data mais recente da fonte."""
    download_source_zip()
    # pyrefly: ignore [bad-argument-type]
    return SourceInspection(reference_date=get_source_max_date())


def microdados_download(download_params: dict) -> ExtractAndLoad:
    """Baixa o ZIP do ano e grava o CSV particionado por `ano=`."""
    year = download_source_zip()
    return ExtractAndLoad(
        coverage=COVERAGE.model_dump(),
        data_path=build_year_file(year),
    )
