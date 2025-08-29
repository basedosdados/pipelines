# -*- coding: utf-8 -*-
"""
Tasks for br_bcb_agencia
"""

import datetime as dt
import os
import zipfile
from datetime import timedelta
from typing import Tuple

import basedosdados as bd
import pandas as pd
from prefect import task

from pipelines.constants import constants
from pipelines.datasets.br_bcb_agencia.constants import (
    constants as agencia_constants,
)
from pipelines.datasets.br_bcb_agencia.utils import (
    check_and_create_column,
    clean_column_names,
    clean_nome_municipio,
    create_cnpj_col,
    download_file,
    fetch_bcb_documents,
    format_date,
    order_cols,
    read_file,
    remove_empty_spaces,
    remove_latin1_accents_from_df,
    remove_non_numeric_chars,
    rename_cols,
    str_to_title,
    strip_dataframe_columns,
)
from pipelines.utils.utils import log, to_partitions


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def get_documents_metadata() -> dict:
    folder = agencia_constants.PASTA.value
    url = agencia_constants.BASE_URL.value
    headers = agencia_constants.HEADERS.value
    params = {
        "tronco": agencia_constants.TRONCO.value,
        "guidLista": agencia_constants.GUID_LISTA.value,
        "ordem": "DataDocumento desc",
        "pasta": folder,
    }
    data = fetch_bcb_documents(url, headers, params)
    return data


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def get_latest_file(data: dict) -> Tuple[str | None, str | None]:
    """
    Extracts the most recent download link from the BCB API JSON structure.

    Args:
        data (dict): JSON loaded from the API

    Returns:
        str: Absolute URL of the most recent file
    """
    documents = data.get("conteudo", [])
    if not documents:
        log("No documents found in the JSON.")
    else:
        # Sort by DataDocumento field (most recent first)
        documents.sort(
            key=lambda d: dt.datetime.fromisoformat(
                d["DataDocumento"].replace("Z", "")
            ),
            reverse=True,
        )

        # Get the first (most recent)
        latest = documents[0]
        relative_url = latest["Url"]
        last_date = dt.datetime.strptime(latest["Titulo"], "%m/%Y").strftime(
            "%Y-%m"
        )
        return (
            agencia_constants.BASE_DOWNLOAD_URL.value + relative_url,
            last_date,
        )


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download_table(
    url: str, download_dir: str = agencia_constants.ZIPFILE_PATH_AGENCIA.value
) -> str:
    if not os.path.exists(download_dir):
        os.makedirs(download_dir, exist_ok=True)

    file_path = download_file(url, download_dir)

    return file_path


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def clean_data():
    """
    This task wrangles the data from the downloaded files.
    """

    ZIP_PATH = agencia_constants.ZIPFILE_PATH_AGENCIA.value
    INPUT_PATH = agencia_constants.INPUT_PATH_AGENCIA.value
    OUTPUT_PATH = agencia_constants.OUTPUT_PATH_AGENCIA.value

    log("Ensuring creation of Input/Output folders")
    if not os.path.exists(INPUT_PATH):
        os.makedirs(INPUT_PATH, exist_ok=True)

    if not os.path.exists(OUTPUT_PATH):
        os.makedirs(OUTPUT_PATH, exist_ok=True)

    zip_files = os.listdir(ZIP_PATH)
    log("Extracting zip files")
    for file in zip_files:
        log(f"File --> : {file}")
        with zipfile.ZipFile(os.path.join(ZIP_PATH, file), "r") as z:
            z.extractall(INPUT_PATH)

    files = os.listdir(INPUT_PATH)

    for file in files:
        if file.endswith(".xls") or file.endswith(".xlsx"):
            file_path = os.path.join(INPUT_PATH, file)
            df = read_file(file_path=file_path, file_name=file)

            # Standardize column names
            df = clean_column_names(df)
            df.rename(columns=rename_cols(), inplace=True)

            # Fill with leading zeros
            df["id_compe_bcb_agencia"] = (
                df["id_compe_bcb_agencia"].astype(str).str.zfill(4)
            )
            df["dv_do_cnpj"] = df["dv_do_cnpj"].astype(str).str.zfill(2)
            df["sequencial_cnpj"] = (
                df["sequencial_cnpj"].astype(str).str.zfill(4)
            )
            df["cnpj"] = df["cnpj"].astype(str).str.zfill(8)
            df["fone"] = df["fone"].astype(str).str.zfill(8)

            # Create columns if they do not exist
            df = check_and_create_column(df, col_name="data_inicio")
            df = check_and_create_column(df, col_name="instituicao")
            df = check_and_create_column(df, col_name="id_instalacao")
            df = check_and_create_column(
                df, col_name="id_compe_bcb_instituicao"
            )
            df = check_and_create_column(df, col_name="id_compe_bcb_agencia")

            # Remove ddd to add later
            df.drop(columns=["ddd"], inplace=True)

            # Some files do not have the 'id_municipio' column, only 'nome'.
            # It is necessary to join by 'nome'
            log("Standardizing municipality names")
            df = clean_nome_municipio(df, "nome")

            municipio = bd.read_sql(
                query="select * from `basedosdados.br_bd_diretorios_brasil.municipio`",
                from_file=True,
                # billing_project_id='basedosdados-dev'
            )
            municipio = municipio[["nome", "sigla_uf", "id_municipio", "ddd"]]

            municipio = clean_nome_municipio(municipio, "nome")
            df["sigla_uf"] = df["sigla_uf"].str.strip()

            if "id_municipio" not in df.columns:
                df = pd.merge(
                    df,
                    municipio[["nome", "sigla_uf", "id_municipio", "ddd"]],
                    left_on=["nome", "sigla_uf"],
                    right_on=["nome", "sigla_uf"],
                    how="left",
                )

            # Check if DDD column exists
            if "ddd" not in df.columns:
                df = pd.merge(
                    df,
                    municipio[["id_municipio", "ddd"]],
                    left_on=["id_municipio"],
                    right_on=["id_municipio"],
                    how="left",
                )

            # Standardize CEP to only numbers
            df["cep"] = df["cep"].astype(str)
            df["cep"] = df["cep"].apply(remove_non_numeric_chars)

            # Create unique CNPJ column
            df = create_cnpj_col(df)
            df["cnpj"] = df["cnpj"].apply(remove_non_numeric_chars)
            df["cnpj"] = df["cnpj"].apply(remove_empty_spaces)

            # Columns to convert to title() (first letter uppercase, rest lowercase)
            col_list_to_title = [
                "endereco",
                "complemento",
                "bairro",
                "nome_agencia",
            ]

            for col in col_list_to_title:
                str_to_title(df, column_name=col)
                log(f"column - {col} converted to title")

            # Remove accents
            df = remove_latin1_accents_from_df(df)

            # Date formatting
            df["data_inicio"] = df["data_inicio"].apply(format_date)

            df = strip_dataframe_columns(df)
            df = df[order_cols()]
            log("Columns ordered")

            to_partitions(
                data=df,
                savepath=OUTPUT_PATH,
                partition_columns=["ano", "mes"],
            )

    return OUTPUT_PATH
