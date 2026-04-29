from datetime import timedelta
from pathlib import Path

import basedosdados as bd
import pandas as pd
from prefect import task

from pipelines.constants import constants
from pipelines.datasets.br_inmet_bdmep.constants import (
    constants as constants_microdados,
)
from pipelines.datasets.br_inmet_bdmep.utils import (
    download_inmet,
    get_clima_info,
    get_date_from_path,
    get_estacao_info,
    get_latest_dowload_link,
    verify_inmet_duplicates,
)
from pipelines.utils.utils import log, to_partitions


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
        year = latest_dowload_link.split("/")[-1].split(".")[0]
    else:
        latest_dowload_link = (
            f"https://portal.inmet.gov.br/uploads/dadoshistoricos/{year}.zip"
        )

    if not (constants_microdados.PATH_INPUT.value / str(year)).exists():
        download_inmet(latest_dowload_link)

    paths = constants_microdados.PATH_INPUT.value.glob(
        constants_microdados.PATH_REGEX.value
    )
    datas = [get_date_from_path(str(path)) for path in paths]

    return max(datas).date()


@task
def get_base_inmet() -> Path:
    """
    Processa dados baixados do INMET na pasta de input (constants_microdados.PATH_INPUT),
    concatena dados de todas as estações e salva o dataframe resultante em arquivos CSV.

    Retorna:
        Path: o caminho para o diretório que contém o arquivo CSV de saída.
    """

    files = constants_microdados.PATH_INPUT.value.glob(
        constants_microdados.PATH_REGEX.value
    )

    df_inmet = pd.concat(
        [get_clima_info(file) for file in files], ignore_index=True
    )
    year = df_inmet.data.max().year
    df_inmet["ano"] = year

    if verify_inmet_duplicates(df_inmet):
        log.warning("Dados duplicados encontrados no dataframe do INMET.")
        df_inmet = df_inmet.drop_duplicates(
            subset=["data", "hora", "id_estacao"]
        )

    # Ordem das colunas
    df_inmet = df_inmet[constants_microdados.COLUMNS_ORDER.value]

    path_output = constants_microdados.PATH_OUTPUT.value / "microdados"
    path_output.mkdir(parents=True, exist_ok=True)
    to_partitions(df_inmet, partition_columns=["ano"], savepath=path_output)

    return path_output


@task
def get_stations_inmet() -> Path:
    """
    Esta task processa os arquivos baixados do INMET na pasta de input (constants_microdados.PATH_INPUT),
    extrai as informações das estações meteorológicas e salva o dataframe resultante em um arquivo CSV.
    Retorna:
        Path: o caminho para o diretório que contém o arquivo CSV de saída.
    """
    files = constants_microdados.PATH_INPUT.value.glob(
        constants_microdados.PATH_REGEX.value
    )
    data_municipios = bd.read_sql(
        query="""
        SELECT id_municipio, centroide, sigla_uf
        FROM `basedosdados.br_bd_diretorios_brasil.municipio`
        """,
        from_file=True,
    )
    dicts_estacoes = [
        get_estacao_info(file, data_municipios=data_municipios)
        for file in files
    ]
    df_estacoes = pd.DataFrame(dicts_estacoes)

    path_output = constants_microdados.PATH_OUTPUT.value / "estacoes"
    path_output.mkdir(parents=True, exist_ok=True)
    df_estacoes.to_csv(path_output / "data.csv", index=False)

    return path_output


@task
def true_task():
    return True
