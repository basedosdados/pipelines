# -*- coding: utf-8 -*-
"""
General purpose functions for the br_cgu_servidores_executivo_federal project
"""

from pipelines.utils.utils import log
from pipelines.datasets.br_cgu_servidores_executivo_federal.constants import constants
from pipelines.utils.apply_architecture_to_dataframe.utils import (
    rename_columns,
    read_architecture_table,
)

import os
import requests
import zipfile
import io
import datetime
import pandas as pd


def make_url(sheet_name: str, date: datetime.date) -> str:
    if date.year <= 2019 and sheet_name not in [
        "Militares",
        "Servidores_BACEN",
        "Servidores_SIAPE",
    ]:
        raise ValueError(f"Invalid {sheet_name} for {date.year}, {date.month}")

    month = f"0{date.month}" if date.month < 10 else date.month
    file = f"{date.year}{month}_{sheet_name}"

    return f"{constants.URL.value}/{file}"


def build_urls(sheet_name: str, dates: list[datetime.date]):
    return [{"url": make_url(sheet_name, date), "date": date} for date in dates]


def download_zip_files_for_sheet(sheet_name: str, sheet_urls: list):
    sheet_input_folder = f"{constants.INPUT.value}/{sheet_name}"

    if not os.path.exists(sheet_input_folder):
        os.mkdir(sheet_input_folder)

    session = requests.Session()
    session.mount("https://", requests.adapters.HTTPAdapter(max_retries=3))  # type: ignore

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.93 Safari/537.36",
    }

    log(f"Starting download for {sheet_name=}")

    for sheet_info in sheet_urls:
        href = sheet_info["url"] + ".zip"
        date = sheet_info["date"]
        year = date.year
        month = date.month

        response = requests.get(href, timeout=1000, stream=True, headers=headers)

        z = zipfile.ZipFile(io.BytesIO(response.content))
        z.extractall(f"{sheet_input_folder}/{year}-{month}")

    log(f"Finished download for {sheet_name=}")


def get_csv_file_by_table_name_and_date(table_name: str, date: datetime.date) -> str:
    if table_name in [
        "aposentados_cadastro",
        "pensionistas_cadastro",
        "servidores_cadastro",
        "reserva_reforma_militares_cadastro",
    ]:
        pattern = "Cadastro"
    elif table_name == "remuneracao":
        pattern = "Remuneracao"
    elif table_name == "observacoes":
        pattern = "Observacoes"
    elif table_name == "afastamentos":
        pattern = "Afastamentos"
    else:
        raise ValueError(f"Not found file pattern for {table_name=}, {date=}")

    month = f"0{date.month}" if date.month < 10 else date.month

    path = f"{date.year}{month}_{pattern}.csv"

    return path


def get_source(table_name: str, source: str) -> str:
    ORIGINS = {
        "aposentados_cadastro": {
            "Aposentados_BACEN": "BACEN",
            "Aposentados_SIAPE": "SIAPE",
        },
        "pensionistas_cadastro": {
            "Pensionistas_SIAPE": "SIAPE",
            "Pensionistas_DEFESA": "Defesa",
            "Pensionistas_BACEN": "BACEN",
        },
        "servidores_cadastro": {
            "Servidores_BACEN": "BACEN",
            "Servidores_SIAPE": "SIAPE",
            "Militares": "Militares",
        },
        "reserva_reforma_militares_cadastro": {
            "Reserva_Reforma_Militares": "Reserva Reforma Militares"
        },
        "remuneracao": {
            "Militares": "Militares",
            "Pensionistas_BACEN": "Pensionistas BACEN",
            "Pensionistas_DEFESA": "Pensionistas DEFESA",
            "Reserva_Reforma_Militares": "Reserva Reforma Militares",
            "Servidores_BACEN": "Servidores BACEN",
            "Servidores_SIAPE": "Servidores SIAPE",
        },
        "afastamentos": {"Servidores_BACEN": "BACEN", "Servidores_SIAPE": "SIAPE"},
        "observacoes": {
            "Aposentados_BACEN": "Aposentados BACEN",
            "Aposentados_SIAPE": "Aposentados SIAPE",
            "Militares": "Militares",
            "Pensionistas_BACEN": "Pensionistas BACEN",
            "Pensionistas_DEFESA": "Pensionistas DEFESA",
            "Pensionistas_SIAPE": "Pensionistas SIAPE",
            "Reserva_Reforma_Militares": "Reserva Reforma Militares",
            "Servidores_BACEN": "Servidores BACEN",
            "Servidores_SIAPE": "Servidores SIAPE",
        },
    }

    return ORIGINS[table_name][source]


def read_and_clean_csv(
    table_name: str, source: str, date: datetime.date
) -> pd.DataFrame:
    csv_path = get_csv_file_by_table_name_and_date(table_name, date)

    path = f"{constants.INPUT.value}/{source}/{date.year}-{date.month}/{csv_path}"

    log(f"Reading {table_name=}, {source=}, {date=} {path=}")

    if not os.path.exists(path):
        log(f"File {path=} dont exists")
        return pd.DataFrame()

    df = pd.read_csv(
        path,
        sep=";",
        encoding="windows-1252",
    )

    url_architecture = constants.ARCH.value[table_name]

    df_architecture = read_architecture_table(url_architecture)

    df = rename_columns(df, df_architecture)

    cols_with_date_type = df_architecture.loc[
        df_architecture["bigquery_type"] == "date", "name"
    ].to_list()

    for col in cols_with_date_type:
        df[col] = df[col].apply(
            lambda value: pd.to_datetime(value, format="%d/%m/%Y")
            if value != "Não informada"
            else None
        )

    df["ano"] = date.year
    df["mes"] = date.month

    if "origem" in df.columns:
        df["origem"] = get_source(table_name, source)

    return df


def process_table(table_info: dict) -> tuple[str, pd.DataFrame]:
    table_name: str = table_info["table_name"]
    sources: list[str] = table_info["sources"]
    dates: list[datetime.date] = table_info["dates"]

    def read_csv_by_source(source: str):
        dfs = [read_and_clean_csv(table_name, source, date) for date in dates]

        return pd.concat(dfs)

    log(f"Processing {table_name=}, {sources=}")

    return (
        table_name,
        pd.concat([read_csv_by_source(source) for source in sources]),
    )
