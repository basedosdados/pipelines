"""
Tasks for br_anp_precos_combustiveis
"""

from datetime import date, timedelta

import pandas as pd
from prefect import task

from pipelines.constants import constants
from pipelines.datasets.br_anp_precos_combustiveis.constants import (
    constants as anp_constants,
)
from pipelines.datasets.br_anp_precos_combustiveis.utils import (
    creating_column_ano,
    download_files,
    get_id_municipio,
    lower_colunm_produto,
    merge_table_id_municipio,
    open_csvs,
    orderning_data_coleta,
    partition_data,
    read_anp_file,
    rename_and_reordening,
    rename_and_to_create_endereco,
    rename_columns,
)


@task
def get_data_source_anp_max_date() -> date:
    """Return the latest ``Data da Coleta`` published by ANP.

    Downloads the GLP weekly file from ANP, parses ``Data da Coleta`` as
    ``%d/%m/%Y`` (coercing unparseable values to ``NaT``), drops null dates
    and returns the maximum.

    Returns:
        date: the most recent collection date present in the source file.
    """
    download_files(
        anp_constants.URLS_DATA.value, anp_constants.PATH_INPUT.value
    )
    df = read_anp_file(anp_constants.URL_GLP.value)
    data_obj = pd.to_datetime(
        df["Data da Coleta"], format="%d/%m/%Y", errors="coerce"
    )
    return data_obj.dropna().dt.date.max()


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download_and_transform():
    download_files(anp_constants.URLS.value, anp_constants.PATH_INPUT.value)

    precos_combustiveis = open_csvs(
        anp_constants.URL_DIESEL_GNV.value,
        anp_constants.URL_GASOLINA_ETANOL.value,
        anp_constants.URL_GLP.value,
    )

    df = get_id_municipio(id_municipio=precos_combustiveis)

    df = merge_table_id_municipio(
        id_municipio=df, pd_precos_combustiveis=precos_combustiveis
    )

    df = rename_and_to_create_endereco(precos_combustiveis=df)

    df = orderning_data_coleta(precos_combustiveis=df)

    df = lower_colunm_produto(precos_combustiveis=df)

    df = creating_column_ano(precos_combustiveis=df)

    df = rename_and_reordening(precos_combustiveis=df)

    df = rename_columns(precos_combustiveis=df)

    return df


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def make_partitions(df):
    partition_data(
        df,
        column_name="data_coleta",
        output_directory=anp_constants.PATH_OUTPUT.value,
    )
    return anp_constants.PATH_OUTPUT.value
