from datetime import datetime, timedelta
from pathlib import Path

import basedosdados as bd
import pandas as pd
from prefect import task

from pipelines.constants import constants
from pipelines.datasets.br_inmet_bdmep.constants import ConstantsMicrodados
from pipelines.datasets.br_inmet_bdmep.utils import (
    download_inmet,
    get_clima_info,
    get_date_from_path,
    get_estacao_info,
    get_latest_dowload_link,
)
from pipelines.utils.metadata.utils import (
    get_api_last_update_date,
    get_api_most_recent_date,
)


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def extract_last_date_from_source(year: str | None = None):
    """
    Extrai última data de atualização dos dados históricos do site do INMET.
    """

    if year is None:
        latest_dowload_link = get_latest_dowload_link()
    else:
        latest_dowload_link = (
            f"https://portal.inmet.gov.br/uploads/dadoshistoricos/{year}.zip"
        )

    download_inmet(latest_dowload_link)

    paths = ConstantsMicrodados.PATH_INPUT.value.glob(
        ConstantsMicrodados.PATH_REGEX.value
    )
    datas = [get_date_from_path(str(path)) for path in paths]

    return max(datas).date()


@task
def get_base_inmet(last_date: datetime | None = None) -> Path:
    """
    Processa dados baixados do INMET na pasta de input (ConstantsMicrodados.PATH_INPUT),
    concatena dados de todas as estações e salva o dataframe resultante em arquivos CSV.

    Retorna:
        Path: o caminho para o diretório que contém o arquivo CSV de saída.
    """

    files = ConstantsMicrodados.PATH_INPUT.value.glob(
        ConstantsMicrodados.PATH_REGEX.value
    )

    df_inmet = pd.concat(
        [get_clima_info(file) for file in files], ignore_index=True
    )

    # Ordem das colunas
    df_inmet = df_inmet[ConstantsMicrodados.COLUMNS_ORDER.value]
    year = df_inmet.data.max().year

    if last_date is not None:
        df_inmet = df_inmet[df_inmet.data >= last_date]

    # Salva o dataframe resultante em um arquivo CSV
    path_output = ConstantsMicrodados.PATH_OUTPUT.value / f"ano={year}"
    path_output.mkdir(parents=True, exist_ok=True)
    file_path = path_output / f"microdados_{year}.csv"
    df_inmet.to_csv(file_path, index=False)

    return file_path


@task
def get_stations_inmet() -> Path:
    """
    Esta task processa os arquivos baixados do INMET na pasta de input (ConstantsMicrodados.PATH_INPUT),
    extrai as informações das estações meteorológicas e salva o dataframe resultante em um arquivo CSV.
    Retorna:
        Path: o caminho para o diretório que contém o arquivo CSV de saída.
    """
    files = ConstantsMicrodados.PATH_INPUT.value.glob(
        ConstantsMicrodados.PATH_REGEX.value
    )

    df_estacoes = [get_estacao_info(file) for file in files]
    df_estacoes = pd.DataFrame(df_estacoes)
    year = df_estacoes.data.max().year

    # Salva o dataframe resultante em um arquivo CSV
    path_output = ConstantsMicrodados.PATH_OUTPUT.value / f"ano={year}"
    path_output.mkdir(parents=True, exist_ok=True)
    file_path = path_output / "estacoes.csv"
    df_estacoes.to_csv(file_path, index=False)

    return file_path


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def get_api_last_date(
    dataset_id: str,
    table_id: str,
    data_source_max_date: datetime,
    date_type: str = "data_max_date",
    date_format: str = "%Y-%m-%d",
):
    """Essa task verifica a data mais atual da tabela no BQ

    Args:
        dataset_id e table_id(string): permite encontrar na api a última data de cobertura
        date_format (str): O formato da data a ser procurado no Django
        data_source_max_date (date): A data mais recente dos dados da fonte original

    Returns:
        date: data mais recente encontrada na tabela no BQ
    """

    if isinstance(data_source_max_date, datetime):
        data_source_max_date = data_source_max_date.date()
    if isinstance(data_source_max_date, str):
        data_source_max_date = datetime.strptime(
            data_source_max_date, date_format
        ).date()
    if isinstance(data_source_max_date, pd.Timestamp):
        data_source_max_date = data_source_max_date.date()

    backend = bd.Backend(graphql_url=constants.API_URL.value["prod"])

    if date_type == "data_max_date":
        data_api = get_api_most_recent_date(
            dataset_id=dataset_id,
            table_id=table_id,
            backend=backend,
            date_format=date_format,
        )

    if date_type == "last_update_date":
        data_api = get_api_last_update_date(
            dataset_id=dataset_id, table_id=table_id, backend=backend
        )

    return data_api
