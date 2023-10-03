# -*- coding: utf-8 -*-
"""
Tasks for br_cgu_servidores_executivo_federal
"""

import datetime
import os

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
from prefect import task

from pipelines.datasets.br_cgu_servidores_executivo_federal.constants import constants
from pipelines.datasets.br_cgu_servidores_executivo_federal.utils import (
    build_urls,
    download_zip_files_for_sheet,
    make_url,
    process_table,
)
from pipelines.utils.utils import extract_last_date, log, to_partitions


@task  # noqa
def download_files(date_start: datetime.date, date_end: datetime.date):
    date_range: list[datetime.date] = pd.date_range(
        date_start, date_end, freq="MS"
    ).to_list()

    dates_before_2020 = [date for date in date_range if date.year < 2020]
    dates_pos_2019 = [date for date in date_range if date.year > 2019]

    urls_for_sheets_before_2020 = {
        sheet: build_urls(sheet, dates_before_2020)
        for sheet in ["Militares", "Servidores_BACEN", "Servidores_SIAPE"]
    }

    urls_for_sheets_after_2019 = {
        sheet: build_urls(sheet, dates_pos_2019) for sheet in constants.SHEETS.value[0]
    }

    for key in urls_for_sheets_after_2019.keys():
        if key in urls_for_sheets_before_2020:
            urls_for_sheets_after_2019[key].extend(urls_for_sheets_before_2020[key])

    valid_sheets = {
        sheet: payload
        for (sheet, payload) in urls_for_sheets_after_2019.items()
        if len(payload) > 0
    }

    log(f"{valid_sheets=}")

    if not os.path.exists(constants.INPUT.value):
        os.mkdir(constants.INPUT.value)

    for sheet_name in valid_sheets:
        download_zip_files_for_sheet(sheet_name, valid_sheets[sheet_name])

    return valid_sheets


@task
def merge_and_clean_data(sheets_info):
    def get_sheets_by_sources(table_name: str, sources: list[str]):
        dates = [
            list(map(lambda i: i["date"], info))
            for (name, info) in sheets_info.items()
            if name in sources
        ]
        return {"table_name": table_name, "sources": sources, "dates": dates[0]}

    tables = set(
        [
            table_name
            for table_name, sources in constants.TABLES.value.items()
            for source in sources
            if source in list(sheets_info.keys())
        ]
    )

    table_and_source = [
        get_sheets_by_sources(table, constants.TABLES.value[table]) for table in tables
    ]

    log(f"{table_and_source=}")

    return [process_table(table_info) for table_info in table_and_source]


@task
def make_partitions(tables: list[tuple[str, pd.DataFrame]]) -> dict[str, str]:
    output = constants.OUTPUT.value

    if not os.path.exists(output):
        os.mkdir(output)

    for table_name, df in tables:
        if len(df) > 0:
            savepath = f"{output}/{table_name}"

            if not os.path.exists(savepath):
                os.mkdir(savepath)

            log(f"{table_name=}")
            log(f"{df.columns=}")
            log(df.head())

            to_partitions(
                data=df,
                partition_columns=["ano", "mes"],
                savepath=savepath,
            )
        else:
            log(f"{table_name=} is empty")

    return {
        table_name: f"{output}/{table_name}" for table_name, df in tables if len(df) > 0
    }


@task
def table_is_available(tables: dict[str, str], table: str) -> bool:
    available = table in tables

    log(f"{table=} not available in {tables.keys()=}")

    return available


@task
def is_up_to_date(next_date: datetime.date) -> bool:
    log(f"Next date: {next_date=}")

    url = make_url("Servidores_SIAPE", next_date)

    session = requests.Session()
    session.mount("https://", requests.adapters.HTTPAdapter(max_retries=3))  # type: ignore

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.93 Safari/537.36",
    }

    response = requests.get(url, timeout=1000, stream=True, headers=headers)

    log(f"Response: {response=}")

    return response.status_code != 200


@task
def get_next_date() -> datetime.date:
    last_date_in_bq = extract_last_date(
        "br_cgu_servidores_executivo_federal",
        "servidores_cadastro",
        "yy-mm",
        "basedosdados-dev",
    )

    year, month = last_date_in_bq.split("-")

    return datetime.date(int(year), int(month), 1) + relativedelta(months=1)
