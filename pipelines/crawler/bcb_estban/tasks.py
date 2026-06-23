"""
Tasks for br_bcb_estban — Prefect 3.
"""

import datetime as dt
import os
import zipfile

import basedosdados as bd
import pandas as pd
from prefect import task

from pipelines.constants import constants
from pipelines.crawler.bcb_estban.constants import (
    constants as br_bcb_estban_constants,
)
from pipelines.crawler.bcb_estban.utils import (
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

TASK_RETRIES = constants.TASK_MAX_RETRIES.value
TASK_RETRY_DELAY_SECONDS = constants.TASK_RETRY_DELAY.value


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def get_documents_metadata(table_id: str) -> dict | None:
    folder = br_bcb_estban_constants.TABLES_CONFIGS.value[table_id]["pasta"]
    url = br_bcb_estban_constants.BASE_URL.value
    headers = br_bcb_estban_constants.HEADERS.value
    params = {
        "tronco": br_bcb_estban_constants.TRONCO.value,
        "guidLista": br_bcb_estban_constants.GUID_LISTA.value,
        "ordem": "DataDocumento desc",
        "pasta": folder,
    }
    return fetch_bcb_documents(url, headers, params)


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def get_latest_file(data: dict) -> tuple[str | None, str | None]:
    """Extracts URL and YYYY-MM date of the most recent document."""
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
    log(
        f"Latest Document: {latest}\nTitle: {latest['Titulo']}\nLast Date: {last_date}"
    )
    return (
        br_bcb_estban_constants.BASE_DOWNLOAD_URL.value + relative_url,
        last_date,
    )


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def download_table(url: str, table_id: str) -> str:
    download_dir = br_bcb_estban_constants.TABLES_CONFIGS.value[table_id][
        "zipfile_path"
    ]
    if not os.path.exists(download_dir):
        os.makedirs(download_dir, exist_ok=True)
    file_path = download_file(url, download_dir)
    log(f"Downloading table to {file_path}")
    return file_path


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def get_id_municipio() -> pd.DataFrame:
    """Carrega o diretório municipio do BD para conversão de id_municipio_bcb."""
    df = bd.read_sql(
        query="select * from `basedosdados.br_bd_diretorios_brasil.municipio`",
        from_file=True,
    )
    df = df[["id_municipio_bcb", "id_municipio"]]
    log("BD directories municipio dataset successfully downloaded!")
    return df


def _validate_date(
    original_date: dt.datetime | str | pd.Timestamp,
    date_format: str = "%Y-%m",
):
    if isinstance(original_date, dt.datetime):
        return original_date.date()
    if isinstance(original_date, str):
        return dt.datetime.strptime(original_date, date_format).date()
    if isinstance(original_date, pd.Timestamp):
        return original_date
    log(
        f"Unable to validate date: {original_date} of type {type(original_date)}.",
        "warning",
    )
    return original_date


@task
def extract_urls_list(
    docs_metadata: dict,
    date_one: dt.datetime | str,
    date_two: dt.datetime | str,
    date_format: str = "%Y-%m",
) -> list[str]:
    """Lista URLs entre as duas datas (exclusivo no menor, inclusivo no maior)."""
    date_one = _validate_date(date_one, date_format)
    date_two = _validate_date(date_two, date_format)
    start, end = (
        (date_one, date_two)
        if date_two >= date_one
        else (
            date_two,
            date_one,
        )
    )
    sorted_docs = sort_documents_by_date(docs_metadata)

    list_result: list[str] = []
    docs_index = 0
    if not sorted_docs:
        return list_result

    current_doc = sorted_docs[docs_index]
    current_date = dt.datetime.strptime(current_doc["Titulo"], "%m/%Y").date()

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


@task
def cleaning_data(table_id: str, df_diretorios: pd.DataFrame) -> str:
    """Descompacta os zips baixados, limpa e particiona em CSV por ano/mes/sigla_uf."""
    zip_path = br_bcb_estban_constants.TABLES_CONFIGS.value[table_id][
        "zipfile_path"
    ]
    input_path = br_bcb_estban_constants.TABLES_CONFIGS.value[table_id][
        "input_path"
    ]
    output_path = br_bcb_estban_constants.TABLES_CONFIGS.value[table_id][
        "output_path"
    ]

    log("Building paths")
    os.makedirs(input_path, exist_ok=True)
    os.makedirs(output_path, exist_ok=True)

    zip_files = os.listdir(zip_path)
    log(f"Unzipping files ----> {zip_files}")
    for file in zip_files:
        if file.endswith(".csv.zip"):
            log(f"Unzipping file ----> : {file}")
            with zipfile.ZipFile(os.path.join(zip_path, file), "r") as z:
                z.extractall(input_path)

    csv_files = os.listdir(input_path)
    for file in csv_files:
        log(f"The file being cleaned is: {file}")
        file_path = os.path.join(input_path, file)

        df_raw = pd.read_csv(
            file_path,
            sep=";",
            index_col=None,
            encoding="latin-1",
            skipfooter=2,
            skiprows=2,
            dtype={"CNPJ": str, "CODMUN": str, "CODMUN_IBGE": str},
        )

        df_raw = clean_dataframe(df_raw)
        df_wide = pre_cleaning_for_pivot_long(df_raw, table_id)
        df_wide = df_wide.merge(
            df_diretorios, on=["id_municipio_bcb"], how="left"
        )
        df_long = wide_to_long(df_wide)
        df_long = standardize_monetary_units(
            df_long, date_column="data_base", value_column="valor"
        )
        df_long = create_id_verbete_column(df_long, column_name="id_verbete")
        df_long = create_month_year_columns(df_long, date_column="data_base")
        df_long["id_municipio"] = df_long["id_municipio"].fillna(
            df_long["id_municipio_original"]
        )
        df_long.loc[
            df_long["municipio"].str.lower().str.startswith("brasilia"),
            ["id_municipio"],
        ] = "5300108"
        df_ordered = order_cols(df_long, table_id)

        log("Saving and partitioning.")
        to_partitions(
            df_ordered,
            partition_columns=["ano", "mes", "sigla_uf"],
            savepath=output_path,
        )
        del (df_wide, df_long, df_raw)

    return output_path
