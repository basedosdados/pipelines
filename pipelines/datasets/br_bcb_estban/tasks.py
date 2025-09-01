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
    create_id_verbete_column,
    create_month_year_columns,
    download_file,
    fetch_bcb_documents,
    order_cols,
    pre_cleaning_for_pivot_long,
    sort_documents_by_date,
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

    df_diretorios = pd.DataFrame(
        df_diretorios, columns=["id_municipio_bcb", "id_municipio"]
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
            dtype={"CNPJ": str, "CODMUN": str, "CODMUN_IBGE": str},
        )

        log("Reading file.")
        df_raw = clean_dataframe(df_raw)
        log("Cleaning dataframe.")
        df_wide = pre_cleaning_for_pivot_long(df_raw, table_id)
        df_wide = df_wide.merge(
            df_diretorios, on=["id_municipio_bcb"], how="left"
        )
        log(f"Pre cleaning for pivot long.{df_wide.columns.to_list()}")
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
        df_long["id_municipio"].fillna(
            df_long["id_municipio_original"], inplace=True
        )
        df_long.loc[
            df_long["municipio"].str.lower().str.startswith("brasilia"),
            ["id_municipio"],
        ] = "5300108"
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


def validate_date(
    original_date: dt.datetime | str | pd.Timestamp,
    date_format: str = "%Y-%m",
):
    if isinstance(original_date, dt.datetime):
        log(f"{original_date} is datetime.")
        return original_date.date()
    if isinstance(original_date, str):
        log(f"{original_date} is string.")
        final_date = dt.datetime.strptime(original_date, date_format)
        log(f"{final_date} transformado em {type(final_date.date())}.")
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
    """ """

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
            br_bcb_estban_constants.BASE_DOWNLOAD_URL.value + relative_url
        )
        docs_index += 1
    log(f"Extracted URLs:{list_result}")
    return list_result
