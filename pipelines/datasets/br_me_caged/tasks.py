# -*- coding: utf-8 -*-
"""
Tasks for br_me_caged
"""

import datetime
import ftplib

# pylint: disable=invalid-name
import re
from datetime import timedelta
from pathlib import Path
from typing import List, Tuple

import basedosdados as bd
import pandas as pd
from dateutil.relativedelta import relativedelta
from prefect import task
from prefect.triggers import all_finished
from tqdm import tqdm
from unidecode import unidecode

from pipelines.constants import constants
from pipelines.datasets.br_me_caged.constants import (
    constants as caged_constants,
)
from pipelines.datasets.br_me_caged.utils import (
    download_file,
    get_caged_schedule,
    verify_yearmonth,
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.metadata.utils import get_api_most_recent_date
from pipelines.utils.utils import log


@task
def build_table_paths(
    table_id: str, parent_dir: str | Path = caged_constants.DATASET_DIR.value
) -> Tuple[Path, Path]:
    parent_dir = Path(parent_dir)
    parent_dir.mkdir(parents=True, exist_ok=True)

    table_dir = parent_dir / table_id
    table_dir.mkdir(exist_ok=True)
    table_input_dir = table_dir / "input"
    table_output_dir = table_dir / "output"

    table_input_dir.mkdir(exist_ok=True)
    table_output_dir.mkdir(exist_ok=True)

    return table_input_dir, table_output_dir


@task
def get_source_last_date(
    ftp_host: str = caged_constants.FTP_HOST.value,
) -> datetime.date:
    """
    This task reaches 'ftp.mtps.gov.br' subfolders looking for most recent year and month

    Parameters:
        ftp_host (str): the FTP host to connect to (default: "ftp.mtps.gov.br")

    Returns:
        last_date (datetime): most recent date with updated content
    """
    ftp = ftplib.FTP(ftp_host)
    ftp.login()
    ftp.encoding = "latin-1"
    ftp.cwd(caged_constants.REMOTE_DIR.value)
    try:
        folder_items = ftp.nlst()
        year_folders = [
            int(re.search(r"\d{4}", item).group(0))
            for item in folder_items
            if re.search(r"\d{4}", item)
        ]

        year_folders.sort(reverse=True)
        ftp.cwd(str(year_folders[0]))
        folder_items = ftp.nlst()
        month_folders = [
            int(re.search(r"^(?:\d{4})(\d{2})$", item).group(1))
            for item in folder_items
            if re.search(r"^(?:\d{4})(\d{2})$", item)
        ]
        month_folders.sort(reverse=True)
        last_date = datetime.datetime(
            year=year_folders[0], month=month_folders[0], day=1
        )
        return last_date.date()
    except Exception as ErrorNlst:
        log(f"Unable to access CAGED subfolders due to {ErrorNlst}")


@task
def get_table_last_date(
    dataset_id: str,
    table_id: str,
) -> datetime.date:
    """
    This task gets most recent date content for a given table

    Args:
        dataset_id e table_id(string): table and dataset identification to fecth correspondent last date

    Returns:
        str: table most recent date
    """

    backend = bd.Backend(graphql_url=utils_constants.API_URL.value["prod"])

    data_api = get_api_most_recent_date(
        dataset_id=dataset_id,
        table_id=table_id,
        backend=backend,
        date_format="%Y-%m",
    )
    data_api = datetime.datetime(year=2020, month=1, day=1).date()
    return data_api


@task
def generate_yearmonth_range(
    start_date: str | datetime.date, end_date: str | datetime.date
) -> list:
    """
    Generate a list of yearmonth strings between start_date and end_date (inclusive).

    Parameters:
    start_date (str): Start date in format 'YYYYMM'
    end_date (str): End date in format 'YYYYMM'

    Returns:
    list: List of yearmonth strings in chronological order

    Raises:
    ValueError: If date format is incorrect or start_date is after end_date
    """
    if isinstance(start_date, datetime.date):
        start_date = start_date.strftime("%Y%m")

    if isinstance(end_date, datetime.date):
        end_date = end_date.strftime("%Y%m")

    # Validate input format
    if not (
        len(start_date) == 6
        and len(end_date) == 6
        and start_date.isdigit()
        and end_date.isdigit()
    ):
        raise ValueError("Dates must be in 'YYYYMM' format")

    # Convert to datetime objects
    start = datetime.datetime.strptime(start_date, "%Y%m")
    end = datetime.datetime.strptime(end_date, "%Y%m")

    # Validate date order
    if start > end:
        raise ValueError("Start date must be before or equal to end date")

    # Generate list of yearmonths
    yearmonths = []
    current = start
    while current <= end:
        yearmonths.append(current.strftime("%Y%m"))
        current += relativedelta(months=1)

    return yearmonths


@task
def crawl_novo_caged_ftp(
    yearmonth: str,
    table_id: str,
    ftp_host: str = caged_constants.FTP_HOST.value,
) -> List:
    """
    Downloads specified .7z files from a CAGED dataset FTP server.

    Parameters:
        yearmonth (str): the month to download data from (e.g., '202301' for January 2023)
        ftp_host (str): the FTP host to connect to (default: "ftp.mtps.gov.br")

    Returns:
        List: Lists of unsuccessfully downloaded files
    """
    verify_yearmonth(yearmonth)
    ftp = ftplib.FTP(ftp_host)
    ftp.login()
    ftp.cwd(f"{caged_constants.REMOTE_DIR.value}/{int(yearmonth[0:4])}/")

    available_months = ftp.nlst()
    if yearmonth not in available_months:
        raise ValueError(
            f"Month {yearmonth} is not available in the directory for the year {yearmonth[0:4]}"
        )

    ftp.cwd(yearmonth)
    log(f"Baixando para o mês: {yearmonth}")

    filenames = [f for f in ftp.nlst() if f.endswith(".7z")]

    successful_downloads = []
    failed_downloads = []

    for file in filenames:
        if "CAGEDMOV" in file and table_id == "microdados_movimentacao":
            log(f"Baixando o arquivo: {file}")
            success, corrupt_file = download_file(
                ftp,
                yearmonth,
                file,
                caged_constants.DATASET_DIR.value
                / "microdados_movimentacao"
                / "input",
            )
            if success:
                successful_downloads.append(file)
            else:
                failed_downloads.append(corrupt_file)

        elif (
            "CAGEDFOR" in file
            and table_id == "microdados_movimentacao_fora_prazo"
        ):
            log(f"Baixando o arquivo: {file}")
            success, corrupt_file = download_file(
                ftp,
                yearmonth,
                file,
                caged_constants.DATASET_DIR.value
                / "microdados_movimentacao_fora_prazo"
                / "input",
            )
            if success:
                successful_downloads.append(file)
            else:
                failed_downloads.append(corrupt_file)

        elif (
            "CAGEDEXC" in file
            and table_id == "microdados_movimentacao_excluida"
        ):
            log(f"Baixando o arquivo: {file}")
            success, corrupt_file = download_file(
                ftp,
                yearmonth,
                file,
                caged_constants.DATASET_DIR.value
                / "microdados_movimentacao_excluida"
                / "input",
            )
            if success:
                successful_downloads.append(file)
            else:
                failed_downloads.append(corrupt_file)

    ftp.quit()

    log("\nDownload Summary:")
    log(f"Successfully downloaded: {successful_downloads}")
    log(f"Failed downloads: {failed_downloads}")

    if len(successful_downloads) == 0:
        log(failed_downloads)
        raise Exception("No successful downloads!")
    return failed_downloads


@task(
    trigger=all_finished,
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def build_partitions(table_id: str, table_output_dir: str | Path) -> str:
    """
    build partitions from gtup files

    table_id: microdados_movimentacao | microdados_movimentacao_fora_prazo | microdados_movimentacao_excluida
    """
    input_files = Path(
        caged_constants.DATASET_DIR.value / table_id / "input"
    ).glob("*txt")
    for filepath in tqdm(input_files):
        log(f"Reading: {filepath}")
        filename = filepath.name
        try:
            df = pd.read_csv(filepath, sep=";", dtype={"uf": str})
            date = re.search(r"\d+", filename).group()
            ano = date[:4]
            mes = int(date[-2:])
            df.columns = [unidecode(col) for col in df.columns]
            df.rename(columns=caged_constants.RENAME_DICT.value, inplace=True)

            log(f"Renaming dataframe columns to: {df.columns}")
            df["sigla_uf"] = df["sigla_uf"].map(caged_constants.UF_DICT.value)

            for state in caged_constants.UF_DICT.value.values():
                log(f"Partitioning for {state}")
                data = df[df["sigla_uf"] == state]
                data.drop(
                    caged_constants.COLUMNS_TO_DROP.value[table_id],
                    axis=1,
                    inplace=True,
                )
                output_dir = (
                    Path(table_output_dir)
                    / f"ano={ano}"
                    / f"mes={mes}"
                    / f"sigla_uf={state}"
                )
                columns_to_select = [
                    col
                    for col in caged_constants.COLUMNS_TO_SELECT.value
                    if col in data.columns
                ]
                output_dir.mkdir(exist_ok=True, parents=True)
                output_path = str(output_dir / "data.csv")
                data[columns_to_select].to_csv(
                    output_path,
                    index=False,
                )
                del data
            del df
        except Exception as e:
            log(f"Failed to read: {filepath} due to: {e}", "error")
    return table_output_dir


@task
def update_caged_schedule(
    table_last_date: str | datetime.date,
    schedules_file: str = "/pipelines/datasets/br_me_caged/schedules.py",
    schedule_url: str = caged_constants.URL_SCHEDULE.value,
):
    """
    This task fetches a specific page to extract html data
    containing CAGED monhtly releasing dates.
    """
    log(f"Looking for schedule info on CAGED releasing on {schedule_url}")
    date_elements = get_caged_schedule(
        url=schedule_url,
        css_selector=caged_constants.CSS_SELECTOR_SCHEDULES.value,
    )
    this_month = datetime.datetime.strptime(table_last_date, "%d/%m/%Y").month
    log(f"This month {this_month}")
    next_start_date = date_elements[0]["data"]
    index = 1
    while index < len(date_elements) - 1:
        log(f"Current next start date {next_start_date}")
        if date_elements[index]["data_competencia"].month > this_month:
            next_start_date = date_elements[index]["data"]
        else:
            break
        index += 1

    with open(schedules_file, "w", encoding="utf-8") as f:
        f.write(f'''# -*- coding: utf-8 -*-
    """
    Schedules for br_me_caged
    """

    from datetime import datetime, timedelta
    from prefect.schedules import Schedule
    from prefect.schedules.clocks import IntervalClock
    from pipelines.constants import constants

    every_month = Schedule(
        clocks=[
            IntervalClock(
                interval=timedelta(days=30),
                start_date=datetime({next_start_date.year}, {next_start_date.month}, {next_start_date.day}),
                labels=[
                    constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
                ],
            )
        ]
    )
    ''')

    return next_start_date
