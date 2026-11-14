"""
Tasks Prefect de br_inmet_bdmep.
"""

from collections.abc import Callable
from datetime import date

from prefect import task

from pipelines.constants import constants as repo_constants
from pipelines.datasets.br_inmet_bdmep.constants import (
    DATASET_ID,
    ESTACAO_TABLE_ID,
    ConstantsEstacao,
    ConstantsMicrodados,
)
from pipelines.datasets.br_inmet_bdmep.utils import (
    build_station_csv,
    build_year_csv,
    download_year_zip,
    get_max_date,
)
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)


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


@task
def build_station_file(year: int) -> str:
    """
    Grava o CSV do cadastro das estações particionado por `ano=`.

    Args:
        year (int): ano do ZIP baixado.

    Returns:
        str: raiz das partições, para o upload.
    """
    build_station_csv(
        input_dir=ConstantsMicrodados.PATH_INPUT.value,
        output_dir=ConstantsEstacao.PATH_OUTPUT.value,
        year=year,
    )
    return str(ConstantsEstacao.PATH_OUTPUT.value)


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    """
    Monta o check_update de `table_id`. As duas tabelas leem a mesma data
    da fonte; a `estacao`, sem coluna de data, compara com a última
    materialização em vez da cobertura.

    Args:
        table_id (str): ID da tabela.

    Returns:
        Callable[[], SourceInspection]: função que baixa o ZIP do ano e
            devolve a data mais recente da fonte.
    """

    def get_latest_update() -> SourceInspection:
        """Baixa o ZIP do ano e devolve a data mais recente da fonte."""
        download_source_zip()
        compare_against = (
            "table_update" if table_id == ESTACAO_TABLE_ID else "coverage"
        )
        return SourceInspection(
            # pyrefly: ignore [bad-argument-type]
            reference_date=get_source_max_date(),
            compare_against=compare_against,
        )

    return get_latest_update


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    """
    Monta o extract_and_load de `table_id`.

    Args:
        table_id (str): ID da tabela.

    Returns:
        Callable[[dict], ExtractAndLoad]: função que baixa o ZIP do ano e
            grava o CSV da tabela particionado por `ano=`.
    """

    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        """Baixa o ZIP do ano e grava o CSV particionado por `ano=`."""
        year = download_source_zip()
        if table_id == ESTACAO_TABLE_ID:
            data_path = build_station_file(year)
        else:
            data_path = build_year_file(year)
        return ExtractAndLoad(
            data_path=data_path,
        )

    return extract_load_data


make_pipeline = pipeline_factory(
    DATASET_ID,
    make_get_latest_update,
    make_extract_load_data,
    date_format="%Y-%m",
)
