import datetime
import ftplib
import re
from pathlib import Path
from typing import List

import py7zr
import requests
from bs4 import BeautifulSoup

from pipelines.datasets.br_me_caged.constants import (
    constants as caged_constants,
)
from pipelines.utils.utils import log


def download_file(
    ftp: ftplib.FTP, remote_dir: str, filename: str, local_dir: str | Path
) -> List:
    """
    Downloads and extracts a .7z file from an FTP server with error handling.

    Parameters:
        ftp (ftplib.FTP): an active FTP connection
        remote_dir (str): the remote directory containing the file
        filename (str): the name of the file to download
        local_dir (str): the local directory to save and extract the file

    Returns:
        bool: True if file downloaded and extracted successfully, False otherwise
    """
    CORRUPT_FILE = []
    local_dir = Path(local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)
    output_path = local_dir / filename

    try:
        with open(output_path, "wb") as f:
            ftp.retrbinary("RETR " + filename, f.write)

        try:
            with py7zr.SevenZipFile(output_path, "r") as archive:
                archive.extractall(path=local_dir)

            output_path.unlink()
            return True, CORRUPT_FILE

        except py7zr.Bad7zFile as extract_error:
            log(f"Error extracting file {filename}: {extract_error}")
            CORRUPT_FILE = {
                "filename": filename,
                "local_path": output_path,
                "error": str(extract_error),
            }
            return False, CORRUPT_FILE

    except Exception as download_error:
        log(f"Error downloading file {filename}: {download_error}")
        CORRUPT_FILE = {
            "filename": filename,
            "local_path": output_path,
            "error": str(download_error),
        }

        log(f"removendo zip corrompido {output_path}")
        if output_path.exists():
            output_path.unlink()

        txt_output_path = Path(str(output_path).replace(".7z", ".txt"))
        log(f"removendo txt corrompido {txt_output_path}")
        if txt_output_path.exists():
            txt_output_path.unlink()
        return False, CORRUPT_FILE


def verify_yearmonth(yearmonth: str):
    if len(yearmonth) != 6 or not yearmonth.isdigit():
        raise ValueError("yearmonth must be a string in the format 'YYYYMM'")


def get_caged_schedule(
    url: str = caged_constants.URL_SCHEDULE.value,
    css_selector: str = caged_constants.CSS_SELECTOR_SCHEDULES.value,
):
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")
    elements = soup.select(css_selector)
    match_elements = [
        re.search(
            r"(\d{2}\/\d{2}\/\d{4})(?:\s?\-\s?Competência\:\s?)(\w+)(?:\s+de\s?)(\d{4})",
            element.text,
        )
        for element in elements
    ]
    date_elements = []
    try:
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
    except Exception as e:
        log(f"Unable to get CAGED schedule: {e}", "error")
    return date_elements
