# -*- coding: utf-8 -*-
"""
Tasks for br_bcb_estban
"""

from prefect import task
import pandas as pd
import os
import basedosdados as bd
import glob

from pipelines.constants import constants
from pipelines.datasets.br_bcb_estban.constants import (
    constants as br_bcb_estban_constants,
)
from datetime import datetime, timedelta

from pipelines.utils.utils import (
    clean_dataframe,
    to_partitions,
    log,
)
from pipelines.datasets.br_bcb_estban.utils import *
from pipelines.datasets.br_bcb_estban.utils import (
    extract_download_links,
    download_and_unzip,
    read_files,
    rename_columns_municipio,
    create_id_municipio,
    pre_cleaning_for_pivot_long_municipio,
    wide_to_long_municipio,
    order_cols_municipio,
    standardize_monetary_units,
    create_id_verbete_column,
    create_month_year_columns,
    rename_columns_agencia,
    pre_cleaning_for_pivot_long_agencia,
    wide_to_long_agencia,
    cols_order_agencia,
    get_data_from_prod,
)


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download_estban_files(xpath: str, save_path: str) -> str:
    """This function downloads ESTBAN data from BACEN url,
    unzip the csv files and return a path for the raw files


    Args:
        xpath (str): The xpath that contains estban file names
        save_path (str): a temporary path to save the estban files

    Returns:
        str: The path to the estban files
    """

    url = br_bcb_estban_constants.ESTBAN_URL.value

    download_link = extract_download_links(url=url, xpath=xpath)

    # setado para fazer upload incremental dos dados em staging
    download_link = download_link[4]

    file = "https://www4.bcb.gov.br/" + download_link
    download_and_unzip(file, path=save_path)

    log("download task successfully !")
    log(f"files {os.listdir(save_path)} were downloaded and unzipped")
    return save_path


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def get_id_municipio(table) -> pd.DataFrame:
    """get id municipio from basedosdados"""

    municipio = get_data_from_prod(
        "br_bd_diretorios_brasil",
        table,
        ["id_municipio_bcb", "id_municipio"],
    )

    municipio = dict(zip(municipio.id_municipio_bcb, municipio.id_municipio))
    log("municipio dataset successfully downloaded!")
    return municipio


# 2. clean data
@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def cleaning_municipios_data(path, municipio):
    """Perform data cleaning operations with estban municipios data

    Args:
        df: a raw municipios estban dataset

    Returns:
        df: a standardized partitioned estban dataset
    """

    files = os.listdir(path)
    log(f"the following files will be cleaned: {files}")

    for file in files:
        log(f"the file being cleaned is:{file}")

        build_complete_file_path = os.path.join(path, file)

        log(f"building {build_complete_file_path}")
        df = read_files(build_complete_file_path)

        log("reading file")
        df = rename_columns_municipio(df)

        log("renaming columns")
        df = clean_dataframe(df)

        log("cleaning dataframe")
        df = create_id_municipio(df, municipio)

        log("creating id municipio")
        df = pre_cleaning_for_pivot_long_municipio(df)

        log("pre cleaning for pivot long")
        df = wide_to_long_municipio(df)

        log("wide to long")
        df = standardize_monetary_units(
            df, date_column="data_base", value_column="valor"
        )

        log("standardizing monetary units")
        df = create_id_verbete_column(df, column_name="id_verbete")

        log("creating id verbete column")
        df = create_month_year_columns(df, date_column="data_base")

        log("creating month year columns")
        df = order_cols_municipio(df)
        # save df

        log("saving and doing partition")
        # 3. build and save partition
        to_partitions(
            df,
            partition_columns=["ano", "mes", "sigla_uf"],
            savepath=br_bcb_estban_constants.CLEANED_FILES_PATH_MUNICIPIO.value,
        )

        del df

    return br_bcb_estban_constants.CLEANED_FILES_PATH_MUNICIPIO.value


# 2. clean data
@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def cleaning_agencias_data(path, municipio):
    """Perform data cleaning operations with estban municipios data

    Args:
        df: a raw municipios estban dataset

    Returns:
        df: a standardized partitioned estban dataset
    """
    # limit to 10 for testing purposes
    # be aware, relie only in .csv files its not that good
    # cause bacen can change file format
    files = os.listdir(path)
    log(f"the following files will be cleaned: {files}")

    for file in files:
        log(f"the file being cleaned is:{file}")
        build_complete_file_path = os.path.join(path, file)

        log(f"building {build_complete_file_path}")
        df = read_files(build_complete_file_path)

        log("reading file")
        df = rename_columns_agencia(df)

        log("renaming columns")
        # see the behavior of the function
        df = clean_dataframe(df)

        log("cleaning dataframe")
        df = create_id_municipio(df, municipio)

        log("creating id municipio")
        df = pre_cleaning_for_pivot_long_agencia(df)

        log("pre cleaning for pivot long")
        df = wide_to_long_agencia(df)

        log("wide to long")
        df = standardize_monetary_units(
            df, date_column="data_base", value_column="valor"
        )
        log("standardizing monetary units")
        df = create_id_verbete_column(df, column_name="id_verbete")

        log("creating id verbete column")
        df = create_month_year_columns(df, date_column="data_base")

        log("creating month year columns")
        df = cols_order_agencia(df)

        to_partitions(
            df,
            partition_columns=["ano", "mes", "sigla_uf"],
            savepath=br_bcb_estban_constants.CLEANED_FILES_PATH_AGENCIA.value,
        )

        del df

    return br_bcb_estban_constants.CLEANED_FILES_PATH_AGENCIA.value
