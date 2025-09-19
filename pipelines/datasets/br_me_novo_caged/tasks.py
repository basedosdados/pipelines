# -*- coding: utf-8 -*-
"""
Tasks for br_me_novo_caged
"""

import datetime
import ftplib

# pylint: disable=invalid-name
import re
from datetime import timedelta
from pathlib import Path
from typing import Tuple

import basedosdados as bd
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta
from prefect import task
from tqdm import tqdm
from unidecode import unidecode

from pipelines.constants import constants
from pipelines.datasets.br_me_novo_caged.constants import (
    constants as caged_constants,
)
from pipelines.datasets.br_me_novo_caged.utils import (
    download_file,
    verify_yearmonth,
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.metadata.utils import get_api_most_recent_date
from pipelines.utils.utils import log


@task
def build_table_paths(
    table_id: str, parent_dir: str | Path = caged_constants.DATASET_DIR.value
) -> Tuple[Path]:
    if not Path(parent_dir).exists():
        parent_dir = Path(parent_dir)
        parent_dir.mkdir(parents=True)

    table_dir = parent_dir / table_id
    table_dir.mkdir()
    table_input_dir = table_dir / "input"
    table_output_dir = table_dir / "output"

    table_input_dir.mkdir()
    table_output_dir.mkdir()

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
) -> bool:
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

    data_api = datetime(year=2024, month=12, day=1).date()
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
):
    """
    Downloads specified .7z files from a CAGED dataset FTP server.

    Parameters:
        yearmonth (str): the month to download data from (e.g., '202301' for January 2023)
        ftp_host (str): the FTP host to connect to (default: "ftp.mtps.gov.br")
        file_types (list): list of file types to download.
                           Options: 'MOV' (movement), 'FOR' (out of deadline), 'EXC' (excluded)
                           If None, downloads all files

    Returns:
        list: List of successfully and unsuccessfully downloaded files
    """
    global CORRUPT_FILES
    CORRUPT_FILES = []

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
            success = download_file(
                ftp,
                yearmonth,
                file,
                caged_constants.DATASET_DIR.value
                / "microdados_movimentacao"
                / "input",
            )
            (successful_downloads if success else failed_downloads).append(
                file
            )

        elif (
            "CAGEDFOR" in file
            and table_id == "microdados_movimentacao_fora_prazo"
        ):
            log(f"Baixando o arquivo: {file}")
            success = download_file(
                ftp,
                yearmonth,
                file,
                caged_constants.DATASET_DIR.value
                / "microdados_movimentacao_fora_prazo"
                / "input",
            )
            (successful_downloads if success else failed_downloads).append(
                file
            )

        elif (
            "CAGEDEXC" in file
            and table_id == "microdados_movimentacao_excluida"
        ):
            log(f"Baixando o arquivo: {file}")
            success = download_file(
                ftp,
                yearmonth,
                file,
                caged_constants.DATASET_DIR.value
                / "microdados_movimentacao_excluida"
                / "input",
            )
            (successful_downloads if success else failed_downloads).append(
                file
            )

    ftp.quit()

    log("\nDownload Summary:")
    log(f"Successfully downloaded: {successful_downloads}")
    log(f"Failed downloads: {failed_downloads}")

    if CORRUPT_FILES:
        log("\nCorrupt Files Details:")
        for corrupt_file in CORRUPT_FILES:
            log(f"Filename: {corrupt_file['filename']}")
            log(f"Local Path: {corrupt_file['local_path']}")
            log(f"Error: {corrupt_file['error']}")
            log("---")


@task(
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
    for filename in tqdm(input_files):
        df = pd.read_csv(filename, sep=";", dtype={"uf": str})
        date = re.search(r"\d+", filename).group()
        ano = date[:4]
        mes = int(date[-2:])

        df.columns = [unidecode(col) for col in df.columns]

        df["uf"] = df["uf"].map(caged_constants.UF_DICT.value)

        for state in caged_constants.UF_DICT.value.values():
            data = df[df["uf"] == state]
            data.drop(["competenciamov", "uf"], axis=1, inplace=True)
            log(df.head(5))
            output_dir = (
                Path(table_output_dir)
                / f"ano={ano}"
                / f"mes={mes}"
                / f"sigla_uf={state}"
            )
            output_dir.mkdir(exist_ok=True, parents=True)
            output_path = str(output_dir / "data.csv")
            data.to_csv(
                output_path,
                index=False,
            )
            del data
        del df

    return table_output_dir


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def get_caged_schedule():
    response = requests.get(caged_constants.URL_SCHEDULE.value)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")
    elements = soup.select(caged_constants.CSS_SELECTOR_SCHEDULES.value)
    match_elements = [
        re.search(
            r"(\d{2}\/\d{2}\/\d{4})(?:\s?\-\s?Competência\:\s?)(\w+)(?:\s+de\s?)(\d{4})",
            element.text,
        )
        for element in elements
    ]

    date_elements = [
        {
            "data": datetime.datetime.strptime(
                match_element.group(1), "%d/%m/%Y"
            ),
            "data_competencia": datetime.datetime.strptime(
                f"01/{caged_constants.FULL_MONTHS.value[match_element.group(2)]}/{match_element.group(3)}",
                "%d/%m/%Y",
            ),
        }
        for match_element in match_elements
        if match_element
    ]
    date_elements.sort(key="competencia", reverse=True)
    return date_elements
