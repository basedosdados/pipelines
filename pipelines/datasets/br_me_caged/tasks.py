# -*- coding: utf-8 -*-
"""
Tasks for br_me_novo_caged
"""

import ftplib
import os

# pylint: disable=invalid-name
import re
from glob import glob

import pandas as pd
import timedelta
from prefect import task
from tqdm import tqdm
from unidecode import unidecode

from pipelines.constants import constants
from pipelines.datasets.br_me_caged.constants import (
    constants as caged_constants,
)
from pipelines.datasets.br_me_caged.utils import (
    download_file,
    verify_yearmonth,
)
from pipelines.utils.utils import log


@task
def crawl_novo_caged_ftp(
    yearmonth: str,
    ftp_host: str = caged_constants.FTP_HOST.value,
    file_types: list = None,
) -> list:
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

    if file_types:
        file_types = [ft.upper() for ft in file_types]
        valid_types = ["MOV", "FOR", "EXC"]
        if not all(ft in valid_types for ft in file_types):
            raise ValueError(f"Invalid file types. Choose from {valid_types}")

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
        if "CAGEDMOV" in file and (not file_types or "MOV" in file_types):
            log(f"Baixando o arquivo: {file}")
            success = download_file(
                ftp,
                yearmonth,
                file,
                "/tmp/caged/microdados_movimentacao/input/",
            )
            (successful_downloads if success else failed_downloads).append(
                file
            )

        elif "CAGEDFOR" in file and (not file_types or "FOR" in file_types):
            log(f"Baixando o arquivo: {file}")
            success = download_file(
                ftp,
                yearmonth,
                file,
                "/tmp/caged/microdados_movimentacao_fora_prazo/input/",
            )
            (successful_downloads if success else failed_downloads).append(
                file
            )

        elif "CAGEDEXC" in file and (not file_types or "EXC" in file_types):
            log(f"Baixando o arquivo: {file}")
            success = download_file(
                ftp,
                yearmonth,
                file,
                "/tmp/caged/microdados_movimentacao_excluida/input/",
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

    return {
        "successful": successful_downloads,
        "failed": failed_downloads,
        "corrupt_files": CORRUPT_FILES,
    }


@task
def get_caged_data(table_id: str, year: int) -> None:
    """
    Get CAGED data
    """
    if year not in [2020, 2021, 2022]:
        raise ValueError("Year must be 2020, 2021 or 2022")
    groups = {
        "microdados_movimentacao": "cagedmov",
        "microdados_movimentacao_fora_prazo": "cagedfor",
        "microdados_movimentacao_excluida": "cageddex",
    }

    group = groups[table_id]
    command = f"bash pipelines/datasets/br_me_caged/bash_scripts/download.sh {group} {table_id} {year}"

    os.system(command)


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def build_partitions(table_id: str) -> str:
    """
    build partitions from gtup files

    table_id: microdados_movimentacao | microdados_movimentacao_fora_prazo | microdados_movimentacao_excluida
    """
    input_files = glob(f"/tmp/caged/{table_id}/input/*txt")
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
            data.to_csv(
                f"/tmp/caged/{table_id}/ano={ano}/mes={mes}/sigla_uf={state}/data.csv",
                index=False,
            )
            del data
        del df

    return f"/tmp/caged/{table_id}/"
