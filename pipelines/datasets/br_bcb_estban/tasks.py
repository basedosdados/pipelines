# -*- coding: utf-8 -*-
"""
Tasks for br_bcb_estban
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
from pipelines.datasets.br_bcb_estban.constants import (
    constants as br_bcb_estban_constants,
)
from pipelines.datasets.br_bcb_estban.utils import (
    create_id_municipio,
    create_id_verbete_column,
    create_month_year_columns,
    download_file,
    fetch_bcb_documents,
    order_cols,
    pre_cleaning_for_pivot_long,
    standardize_monetary_units,
    wide_to_long,
)
from pipelines.utils.utils import clean_dataframe, log, to_partitions


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def get_documents_metadata(table_id: str) -> dict:
    folder = br_bcb_estban_constants.TABLES_CONFIGS.value[table_id]["pasta"]
    url = br_bcb_estban_constants.BASE_URL.value
    headers = br_bcb_estban_constants.HEADERS.value
    params = {
        "tronco": br_bcb_estban_constants.TRONCO.value,
        "guidLista": br_bcb_estban_constants.GUID_LISTA.value,
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
        log(
            f"Latest Document: {latest}\nTitle: {latest['Titulo']}\nLast Date: {last_date}"
        )
        return (
            br_bcb_estban_constants.BASE_DOWNLOAD_URL.value + relative_url,
            last_date,
        )


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download_table(url: str, table_id: str) -> str:
    download_dir = br_bcb_estban_constants.TABLES_CONFIGS.value[table_id][
        "zipfile_path"
    ]
    if not os.path.exists(download_dir):
        os.makedirs(download_dir, exist_ok=True)
    file_path = download_file(url, download_dir)

    log(f"Downloading table to {file_path}")
    return file_path


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def get_id_municipio() -> pd.DataFrame:
    """
    Get id_municipio from basedosdados

    Returns:
        dict: Dictionary mapping id_municipio_bcb to id_municipio
    """

    df_diretorios = bd.read_sql(
        query="select * from `basedosdados.br_bd_diretorios_brasil.municipio`",
        from_file=True,
    )

    df_diretorios = df_diretorios[["id_municipio_bcb", "id_municipio"]]

    df_diretorios = dict(
        zip(df_diretorios.id_municipio_bcb, df_diretorios.id_municipio)
    )
    log("BD directories municipio dataset successfully downloaded!")
    return df_diretorios


@task
def cleaning_data(table_id: str, df_diretorios: pd.DataFrame) -> str:
    """
    Perform data cleaning operations with the dataset.

    Args:
        table_id (str): Table identifier.
        df_diretorios (pd.DataFrame): DataFrame with municipality directories.

    Returns:
        str: File path to a standardized partitioned estban dataset.
    """
    ZIP_PATH = br_bcb_estban_constants.TABLES_CONFIGS.value[table_id][
        "zipfile_path"
    ]
    INPUT_PATH = br_bcb_estban_constants.TABLES_CONFIGS.value[table_id][
        "input_path"
    ]
    OUTPUT_PATH = br_bcb_estban_constants.TABLES_CONFIGS.value[table_id][
        "output_path"
    ]

    log("Building paths")
    if not os.path.exists(INPUT_PATH):
        os.makedirs(INPUT_PATH, exist_ok=True)

    if not os.path.exists(OUTPUT_PATH):
        os.makedirs(OUTPUT_PATH, exist_ok=True)

    zip_files = os.listdir(ZIP_PATH)
    log(f"Unzipping files ----> {zip_files}")
    for file in zip_files:
        if file.endswith(".csv.zip"):
            log(f"Unzipping file ----> : {file}")
            with zipfile.ZipFile(os.path.join(ZIP_PATH, file), "r") as z:
                z.extractall(INPUT_PATH)

    csv_files = os.listdir(INPUT_PATH)

    for file in csv_files:
        log(f"The file being cleaned is: {file}")

        file_path = os.path.join(INPUT_PATH, file)

        log(f"Building {file_path}")

        df_raw = pd.read_csv(
            file_path,
            sep=";",
            index_col=None,
            encoding="latin-1",
            skipfooter=2,
            skiprows=2,
            dtype={"CNPJ": str, "CODMUN": str},
        )

        log("Reading file.")
        df_raw = clean_dataframe(df_raw)
        log("Cleaning dataframe.")
        df_wide = create_id_municipio(df_raw, df_diretorios)
        df_wide = pre_cleaning_for_pivot_long(df_wide, table_id)
        log("Pre cleaning for pivot long.")
        df_long = wide_to_long(df_wide)
        log("Wide to long.")
        df_long = standardize_monetary_units(
            df_long, date_column="data_base", value_column="valor"
        )
        log("Standardizing monetary units.")
        df_long = create_id_verbete_column(df_long, column_name="id_verbete")
        log("Creating id_verbete column.")
        df_long = create_month_year_columns(df_long, date_column="data_base")
        log("Creating month and year columns.")
        df_ordered = order_cols(df_long, table_id)
        log("Saving and partitioning.")

        # Build and save partition
        to_partitions(
            df_ordered,
            partition_columns=["ano", "mes", "sigla_uf"],
            savepath=OUTPUT_PATH,
        )

        del (df_wide, df_long, df_raw)

    return OUTPUT_PATH
