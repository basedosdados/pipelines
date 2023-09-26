# -*- coding: utf-8 -*-
"""
Tasks for br_cgu_servidores_executivo_federal
"""

from prefect import task

from pipelines.utils.utils import to_partitions

import pandas as pd
import datetime
import os

from pipelines.datasets.br_cgu_servidores_executivo_federal.constants import constants
from pipelines.datasets.br_cgu_servidores_executivo_federal.utils import (
    build_urls,
    download_zip_files_for_sheet,
    process_table,
)


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

    if not os.path.exists(constants.INPUT.value):
        os.mkdir(constants.INPUT.value)

    for sheet_name in urls_for_sheets_after_2019:
        download_zip_files_for_sheet(sheet_name, urls_for_sheets_after_2019[sheet_name])

    return urls_for_sheets_after_2019


@task
def merge_and_clean_data(sheets_info):
    def get_sheets_by_sources(table_name: str, sources: list[str]):
        dates = [
            list(map(lambda i: i["date"], info))
            for (name, info) in sheets_info.items()
            if name in sources
        ]
        return {"table_name": table_name, "sources": sources, "dates": dates[0]}

    sheets_by_source = [
        get_sheets_by_sources(table_name, sources)
        for table_name, sources in constants.TABLES.value.items()
    ]

    return [process_table(table_info) for table_info in sheets_by_source]


@task
def make_partitions(tables: list[tuple[str, pd.DataFrame]]) -> dict[str, str]:
    output = constants.OUTPUT.value

    if not os.path.exists(output):
        os.mkdir(output)

    for table_name, df in tables:
        savepath = f"{output}/{table_name}"

        if not os.path.exists(savepath):
            os.mkdir(savepath)

        to_partitions(
            data=df,
            partition_columns=["ano", "mes"],
            savepath=savepath,
        )

    return {table_name: f"{output}/{table_name}" for table_name, _ in tables}
