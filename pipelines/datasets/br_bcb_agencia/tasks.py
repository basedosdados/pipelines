# -*- coding: utf-8 -*-
"""
Tasks for br_bcb_agencia
"""


import os
import pandas as pd
from datetime import timedelta

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
    order_cols,
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
from pipelines.constants import constants


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download_data(url, xpath):
    # url= agencia_constants.URL_AGENCIA.value
    # xpath = agencia_constants.AGENCIA_XPATH.value
    # extract all download links and select the most recent
    links = extract_download_links(url=url, xpath=xpath)

    link = links[0]

    # select the most recent link
    current_link = "https://www.bcb.gov.br" + link

    log(f"Downloading file from: {current_link} ")

    download_and_unzip(
        url=current_link, extract_to=agencia_constants.DOWNLOAD_PATH_AGENCIA.value
    )

    log(
        f"The file: {os.listdir(agencia_constants.DOWNLOAD_PATH_AGENCIA.value)} was downloaded"
    )


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def clean_data():
    """
    This task wrang the data from the downloaded files
    """
    # set those as constants
    dir_path = agencia_constants.CLEANED_FILES_PATH_AGENCIA.value
    path = agencia_constants.DOWNLOAD_PATH_AGENCIA.value

    files = os.listdir(path)
    log(f"file is: {files}")
    # file = file[1]
    log(f"file is: {files}")

    for file in files:
        # the files format change across the year
        if file.endswith(".xls") or file.endswith(".xlsx"):
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

            # order columns
            df = df[order_cols()]
            log("cols ordered")

            to_partitions(
                data=df,
                savepath=dir_path,
                partition_columns=["ano", "mes"],
            )

    return agencia_constants.CLEANED_FILES_PATH_AGENCIA.value
