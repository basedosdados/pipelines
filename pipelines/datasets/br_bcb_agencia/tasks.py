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
    sort_documents_by_date,
    str_to_title,
    strip_dataframe_columns,
)
from pipelines.utils.metadata.tasks import get_api_most_recent_date
from pipelines.utils.metadata.utils import get_url
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
    Extract the most recent download link and its reference date from BCB metadata.

    Args:
        data (dict): JSON loaded from the BCB API.

    Returns:
        tuple[str | None, str | None]:
            - Absolute URL of the most recent file.
            - Date string in YYYY-MM format.
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
def get_api_max_date(dataset_id, table_id):
    """
    Get the most recent date available in the API for a given dataset/table.

    Args:
        dataset_id (str): ID of the dataset in Basedosdados.
        table_id (str): ID of the table in Basedosdados.

    Returns:
        str: Maximum date available in the API, formatted as "%Y-%m".
    """
    backend = bd.Backend(graphql_url=get_url("prod"))
    api_max_date = get_api_most_recent_date(
        dataset_id=dataset_id,
        table_id=table_id,
        backend=backend,
        date_format="%Y-%m",
    )

    return api_max_date


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download_table(
    url: str, download_dir: str = agencia_constants.ZIPFILE_PATH_AGENCIA.value
) -> str:
    """
    Download a ZIP file from the given URL and save it to the specified directory.

    Args:
        url (str): Download URL for the ZIP file.
        download_dir (str, optional): Local path to store the file.
            Defaults to `ZIPFILE_PATH_AGENCIA`.

    Returns:
        str: Path to the downloaded file.
    """
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


def validate_date(
    original_date: dt.datetime | str | pd.Timestamp,
    date_format: str = "%Y-%m",
):
    """
    Normalize date input to a `datetime.date` object.

    Args:
        original_date (datetime | str | pd.Timestamp): Date input in different formats.
        date_format (str, optional): Format to parse strings. Defaults to "%Y-%m".

    Returns:
        datetime.date | pd.Timestamp: Normalized date object.
    """
    if isinstance(original_date, dt.datetime):
        return original_date.date()
    if isinstance(original_date, str):
        final_date = dt.datetime.strptime(original_date, date_format)
        return final_date.date()
    if isinstance(original_date, pd.Timestamp):
        return original_date
    log("Unable to validate date.", "warning")
    return original_date


@task
def extract_urls_list(
    docs_metadata: dict,
    date_one: dt.datetime | str,
    date_two: dt.datetime | str,
    date_format: str = "%Y-%m",
) -> list:
    """
    Extract list of document download URLs between two reference dates.

    Args:
        docs_metadata (dict): Metadata JSON returned from BCB API.
        date_one (datetime | str): Start date.
        date_two (datetime | str): End date.
        date_format (str, optional): Expected format for parsing string dates.
            Defaults to "%Y-%m".

    Returns:
        list[str]: List of absolute URLs within the given date range.
    """

    date_one = validate_date(date_one, date_format)
    date_two = validate_date(date_two, date_format)
    if date_two >= date_one:
        start, end = date_one, date_two
    else:
        start, end = date_two, date_one
    sorted_docs = sort_documents_by_date(docs_metadata)

    # First step
    docs_index = 0
    current_doc = sorted_docs[docs_index]
    relative_url = current_doc["Url"]
    current_date = dt.datetime.strptime(current_doc["Titulo"], "%m/%Y").date()
    list_result = []
    while (
        (current_date <= end)
        and (current_date > start)
        and docs_index < len(sorted_docs)
    ):
        current_doc = sorted_docs[docs_index]
        relative_url = current_doc["Url"]
        current_date = dt.datetime.strptime(
            current_doc["Titulo"], "%m/%Y"
        ).date()
        log(f"Current Document URL: {relative_url}\nLast Date: {current_date}")
        list_result.append(
            agencia_constants.BASE_DOWNLOAD_URL.value + relative_url
        )
        docs_index += 1
    log(f"Extracted URLs:{list_result}")
    return list_result
