"""
Tasks for br_bcb_agencia — Prefect 3.
"""

import datetime as dt
import os
import zipfile

import basedosdados as bd
import pandas as pd
from prefect import task

from pipelines.crawler.bcb_agencia.constants import (
    constants as agencia_constants,
)
from pipelines.crawler.bcb_agencia.utils import (
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
from pipelines.utils.utils import log, to_partitions


@task(retries=5, retry_delay_seconds=10)
def get_documents_metadata() -> dict | None:
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


@task(retries=5, retry_delay_seconds=10)
def get_latest_file(data: dict) -> tuple[str | None, str | None]:
    """
    Extract the most recent download link and its reference date from BCB metadata.
    """
    documents = data.get("conteudo", [])
    if not documents:
        log("No documents found in the JSON.")
        return None, None

    documents.sort(
        key=lambda d: dt.datetime.fromisoformat(
            d["DataDocumento"].replace("Z", "")
        ),
        reverse=True,
    )

    latest = documents[0]
    relative_url = latest["Url"]
    last_date = dt.datetime.strptime(latest["Titulo"], "%m/%Y").strftime(
        "%Y-%m"
    )
    return (
        agencia_constants.BASE_DOWNLOAD_URL.value + relative_url,
        last_date,
    )


@task(retries=5, retry_delay_seconds=10)
def download_table(
    url: str, download_dir: str = agencia_constants.ZIPFILE_PATH_AGENCIA.value
) -> str:
    if not os.path.exists(download_dir):
        os.makedirs(download_dir, exist_ok=True)

    file_path = download_file(url, download_dir)
    return file_path


@task(retries=5, retry_delay_seconds=10)
def clean_data() -> str:
    """
    Wrangles the data from the downloaded files and writes partitioned CSVs.
    """
    zip_path = agencia_constants.ZIPFILE_PATH_AGENCIA.value
    input_path = agencia_constants.INPUT_PATH_AGENCIA.value
    output_path = agencia_constants.OUTPUT_PATH_AGENCIA.value

    log("Ensuring creation of Input/Output folders")
    if not os.path.exists(input_path):
        os.makedirs(input_path, exist_ok=True)

    if not os.path.exists(output_path):
        os.makedirs(output_path, exist_ok=True)

    zip_files = os.listdir(zip_path)
    log("Extracting zip files")
    for file in zip_files:
        log(f"File --> : {file}")
        with zipfile.ZipFile(os.path.join(zip_path, file), "r") as z:
            z.extractall(input_path)

    files = os.listdir(input_path)

    for file in files:
        if file.endswith(".xls") or file.endswith(".xlsx"):
            file_path = os.path.join(input_path, file)
            df = read_file(file_path=file_path, file_name=file)

            df = clean_column_names(df)
            df = df.rename(columns=rename_cols())

            df["id_compe_bcb_agencia"] = (
                df["id_compe_bcb_agencia"].astype(str).str.zfill(4)
            )
            df["dv_do_cnpj"] = df["dv_do_cnpj"].astype(str).str.zfill(2)
            df["sequencial_cnpj"] = (
                df["sequencial_cnpj"].astype(str).str.zfill(4)
            )
            df["cnpj"] = df["cnpj"].astype(str).str.zfill(8)
            df["fone"] = df["fone"].astype(str).str.zfill(8)

            df = check_and_create_column(df, col_name="data_inicio")
            df = check_and_create_column(df, col_name="instituicao")
            df = check_and_create_column(df, col_name="id_instalacao")
            df = check_and_create_column(
                df, col_name="id_compe_bcb_instituicao"
            )
            df = check_and_create_column(df, col_name="id_compe_bcb_agencia")

            df = df.drop(columns=["ddd"])

            log("Standardizing municipality names")
            df = clean_nome_municipio(df, "nome")

            municipio = bd.read_sql(
                query="select * from `basedosdados.br_bd_diretorios_brasil.municipio`",
                from_file=True,
            )
            municipio = municipio[["nome", "sigla_uf", "id_municipio", "ddd"]]

            municipio = clean_nome_municipio(municipio, "nome")
            df["sigla_uf"] = df["sigla_uf"].str.strip()

            if "id_municipio" not in df.columns:
                df = df.merge(
                    municipio[["nome", "sigla_uf", "id_municipio", "ddd"]],
                    left_on=["nome", "sigla_uf"],
                    right_on=["nome", "sigla_uf"],
                    how="left",
                )

            if "ddd" not in df.columns:
                df = df.merge(
                    municipio[["id_municipio", "ddd"]],
                    left_on=["id_municipio"],
                    right_on=["id_municipio"],
                    how="left",
                )

            df["cep"] = df["cep"].astype(str)
            df["cep"] = df["cep"].apply(remove_non_numeric_chars)

            df = create_cnpj_col(df)
            df["cnpj"] = df["cnpj"].apply(remove_non_numeric_chars)
            df["cnpj"] = df["cnpj"].apply(remove_empty_spaces)

            col_list_to_title = [
                "endereco",
                "complemento",
                "bairro",
                "nome_agencia",
            ]

            for col in col_list_to_title:
                str_to_title(df, column_name=col)
                log(f"column - {col} converted to title")

            df = remove_latin1_accents_from_df(df)

            df["data_inicio"] = df["data_inicio"].apply(format_date)

            df = strip_dataframe_columns(df)
            df = df[order_cols()]
            log("Columns ordered")

            to_partitions(
                data=df,
                savepath=output_path,
                partition_columns=["ano", "mes"],
            )

    return output_path


def validate_date(
    original_date: dt.datetime | str | pd.Timestamp,
    date_format: str = "%Y-%m",
):
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
    """
    date_one = validate_date(date_one, date_format)
    date_two = validate_date(date_two, date_format)
    if date_two >= date_one:
        start, end = date_one, date_two
    else:
        start, end = date_two, date_one
    sorted_docs = sort_documents_by_date(docs_metadata)

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
