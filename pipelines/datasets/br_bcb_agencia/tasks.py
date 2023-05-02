# -*- coding: utf-8 -*-
"""
Tasks for br_bcb_agencia
"""

###############################################################################
#
# Aqui é onde devem ser definidas as tasks para os flows do projeto.
# Cada task representa um passo da pipeline. Não é estritamente necessário
# tratar todas as exceções que podem ocorrer durante a execução de uma task,
# mas é recomendável, ainda que não vá implicar em  uma quebra no sistema.
# Mais informações sobre tasks podem ser encontradas na documentação do
# Prefect: https://docs.prefect.io/core/concepts/tasks.html
#
# De modo a manter consistência na codebase, todo o código escrito passará
# pelo pylint. Todos os warnings e erros devem ser corrigidos.
#
# As tasks devem ser definidas como funções comuns ao Python, com o decorador
# @task acima. É recomendado inserir type hints para as variáveis.
#
# Um exemplo de task é o seguinte:
#
# -----------------------------------------------------------------------------
# from prefect import task
#
# @task
# def my_task(param1: str, param2: int) -> str:
#     """
#     My task description.
#     """
#     return f'{param1} {param2}'
# -----------------------------------------------------------------------------
#
# Você também pode usar pacotes Python arbitrários, como numpy, pandas, etc.
#
# -----------------------------------------------------------------------------
# from prefect import task
# import numpy as np
#
# @task
# def my_task(a: np.ndarray, b: np.ndarray) -> str:
#     """
#     My task description.
#     """
#     return np.add(a, b)
# -----------------------------------------------------------------------------
#
# Abaixo segue um código para exemplificação, que pode ser removido.
#
###############################################################################

import os
import pandas as pd
import openpyxl

from pipelines.datasets.br_bcb_agencia.constants import (
    constants as agencia_constants,
)

from pipelines.datasets.br_bcb_agencia.utils import (
    extract_download_links,
    download_and_unzip,
    read_file,
    clean_column_names,
    rename_cols,
    check_and_create_column,
    clean_nome_municipio,
    get_data_from_prod,
    find_cnpj_row_number,
    remove_non_numeric_chars,
    remove_empty_spaces,
    format_date,
    remove_latin1_accents_from_df,
    strip_dataframe_columns,
    str_to_title,
    create_cnpj_col,
)

from prefect import task
from pipelines.utils.utils import (
    log,
    to_partitions,
)


@task
def download_data(url, xpath):

    # url= agencia_constants.URL_AGENCIA.value
    # xpath = agencia_constants.AGENCIA_XPATH.value
    # extract all download links and select the most recent
    links = extract_download_links(url=url, xpath=xpath)

    # select the most recent link
    links = "https://www.bcb.gov.br" + links[0]

    log(f"Downloading file from: {links} ")

    # download and unzip the file
    download_and_unzip(
        url=links, extract_to=agencia_constants.DOWNLOAD_PATH_AGENCIA.value
    )


# 2. task wrang data
@task
def clean_data():
    """
    This task wrang the data from the downloaded files
    """
    # set those as constants
    dir_path = agencia_constants.CLEANED_FILES_PATH_AGENCIA.value
    path = agencia_constants.DOWNLOAD_PATH_AGENCIA.value

    file = os.listdir(path)
    file = file[0]

    # the files format change across the year
    if file.endswith(".xls") or file.endswith(".xlsx"):

        try:
            file_path = os.path.join(path, file)
            df = read_file(file_path=file_path, file_name=file)
            # general columns stardantization
            df = clean_column_names(df)
            # rename columns
            df.rename(columns=rename_cols(), inplace=True)
            log(df.columns)
            # fill left zeros with field range
            df["id_compe_bcb_agencia"] = (
                df["id_compe_bcb_agencia"].astype(str).str.zfill(4)
            )
            df["dv_do_cnpj"] = df["dv_do_cnpj"].astype(str).str.zfill(2)
            df["sequencial_cnpj"] = df["sequencial_cnpj"].astype(str).str.zfill(4)
            df["cnpj"] = df["cnpj"].astype(str).str.zfill(8)
            df["fone"] = df["fone"].astype(str).str.zfill(8)

            # check existence and create columns
            df = check_and_create_column(df, col_name="data_inicio")
            df = check_and_create_column(df, col_name="instituicao")
            df = check_and_create_column(df, col_name="id_instalacao")
            df = check_and_create_column(df, col_name="id_compe_bcb_instituicao")
            df = check_and_create_column(df, col_name="id_compe_bcb_agencia")

            # drop ddd column thats going to be added later
            df.drop(columns=["ddd"], inplace=True)
            log("ddd removed")
            log(df.columns)
            # some files doesnt have 'id_municipio', just 'nome'.
            # to add new ids is necessary to join by name
            # clean nome municipio

            # todo : copy from estban
            df = clean_nome_municipio(df)

            municipio = get_data_from_prod(
                "br_bd_diretorios_brasil",
                "municipio",
                ["nome", "sigla_uf", "id_municipio", "ddd"],
            )

            log("municipio dataset successfully downloaded!")

            municipio = clean_nome_municipio(municipio)
            # check if id_municipio already exists

            if "id_municipio" not in df.columns:
                # read municipio from bd datalake

                # join id_municipio to df
                df = pd.merge(
                    df,
                    municipio[["nome", "sigla_uf", "id_municipio", "ddd"]],
                    left_on=["nome", "sigla_uf"],
                    right_on=["nome", "sigla_uf"],
                    how="left",
                )

            # check if ddd already exists, if it doesnt, add it
            if "ddd" not in df.columns:
                #
                # read municipio from bd datalake

                df = pd.merge(
                    df,
                    municipio[["id_municipio", "ddd"]],
                    left_on=["id_municipio"],
                    right_on=["id_municipio"],
                    how="left",
                )

            # clean cep column
            df["cep"] = df["cep"].astype(str)
            df["cep"] = df["cep"].apply(remove_non_numeric_chars)
            log("cep ok")

            # cnpj cleaning working
            df = create_cnpj_col(df)

            df["cnpj"] = df["cnpj"].apply(remove_non_numeric_chars)
            df["cnpj"] = df["cnpj"].apply(remove_empty_spaces)

            # select cols to title
            col_list_to_title = ["endereco", "complemento", "bairro", "nome_agencia"]

            for col in col_list_to_title:
                str_to_title(df, column_name=col)
                log(f"column - {col} converted to title")

            # remove latin1 accents from all cols
            df = remove_latin1_accents_from_df(df)

            # format data_inicio
            df["data_inicio"] = df["data_inicio"].apply(format_date)

            # strip all df columns
            df = strip_dataframe_columns(df)

            # make sure month (mes) is an int to use as partition
            ano = df["ano"][1]
            mes = df["ano"][1]

            log(df.columns)
            log("ddd ordeder")

            to_partitions(
                data=df,
                savepath=dir_path,
                partition_columns=["ano", "mes"],
            )

        # keep track of possible erros that may occur
        except Exception as e:
            log(f"error: {e}")

    return f"tmp/ouput/ano={ano}/mes={mes}/data.csv"
