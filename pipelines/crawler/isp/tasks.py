# -*- coding: utf-8 -*-
import os
from datetime import timedelta

import basedosdados as bd
import pandas as pd
from prefect import task

from pipelines.constants import constants
from pipelines.crawler.isp.constants import (
    QUERY,
)
from pipelines.crawler.isp.constants import (
    constants as isp_constants,
)
from pipelines.crawler.isp.utils import (
    change_columns_name,
    check_tipo_fase,
    create_columns_order,
    download_files,
)
from pipelines.utils.utils import log


def read_data(file_name: str) -> pd.DataFrame:
    """This function reads a csv or excel file and returns a dataframe

    Args:
        file_path (str): path to the file

    Returns:
        pd.DataFrame: dataframe with the data
    """

    input = isp_constants.INPUT_PATH.value
    path_file = isp_constants.dict_table.value[file_name]["name_table"]
    file_path = os.path.join(input, path_file)

    if file_path.endswith(".csv"):
        df = pd.read_csv(
            file_path,
            encoding="latin-1",
            sep=";",
            thousands=".",
            decimal=",",
            dtype=str,
        )
        log(f"file -> {file_path} read")
    else:
        df = pd.read_excel(
            file_path,
            thousands=".",
            decimal=",",
        )
        log(f"file -> {file_path} read")

    return df


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def clean_data(
    file_name: str,
):
    output_path = f"{isp_constants.OUTPUT_PATH.value}{file_name}.csv"

    df = read_data(file_name=file_name)
    log("renaming columns")
    # rename columns
    link_arquitetura = isp_constants.dict_table.value[file_name]["sheets_name"]
    nomes_colunas = change_columns_name(link_arquitetura)
    df.rename(columns=nomes_colunas, inplace=True)

    log("creating columns order")
    ordem_colunas = create_columns_order(nomes_colunas)

    log("ordering columns")
    df = df[ordem_colunas]

    log("checking tipo_fase col")
    df = check_tipo_fase(df)

    log("building dir")
    os.makedirs(isp_constants.OUTPUT_PATH.value, exist_ok=True)

    df.to_csv(
        output_path,
        index=False,
        na_rep="",
        encoding="utf-8",
    )
    log(f"df {file_name} salvo com sucesso")

    return output_path


@task
def get_count_lines(file_name: str) -> bool:
    input = isp_constants.INPUT_PATH.value
    path_file = isp_constants.dict_table.value[file_name]["name_table"]

    download_files(file_name=path_file, save_dir=input)

    if file_name == "armas_apreendidas_mensal":
        return True

    df = read_data(file_name=file_name)

    data = bd.read_sql(
        QUERY(file_name),
        billing_project_id="basedosdados",
        from_file=True,
    )

    data = int(data["total"][0])
    df = int(df.shape[0])

    log(f"Quantidade de linhas no BigQuery: {data}")
    log(f"Quantidade de linhas no arquivo: {df}")
    if df >= data:
        log("Quantidade de linhas OK")
        return True

    log("Não há novas linhas. ")
    return False
