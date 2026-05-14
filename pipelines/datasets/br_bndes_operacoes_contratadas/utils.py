import datetime
import os
import re
from pathlib import Path

import pandas as pd
import requests

from pipelines.datasets.br_bndes_operacoes_contratadas.constants import (
    constants,
)
from pipelines.utils.utils import log

last_dataset_update = datetime.datetime.strptime("01/02/2026", "%d/%m/%Y")


def download_xlsx(
    filename: str,
    input_path: Path | str,
    url: str = constants.URL_OPERACOES_CONTRATADAS.value,
) -> Path:
    """
    Makes a GET request to the url and saves the content as an xlsx file in the input_path with the given filename.
    If the file already exists, it will be overwritten.

    Args:
        filename (str): The name of the file to be saved (without extension).
        input_path (Path|str): The path where the file will be saved.
        url (str): The url to make the GET request. Default is constants.URL_OPERACOES_CONTRATADAS.value.
    Returns:
        Path: The path to the downloaded xlsx file.
    Raises:
        HTTPError: If the GET request to the url fails.
    """

    input_file = input_path / f"{filename}.xlsx"
    response = requests.get(url)
    response.raise_for_status()

    response = requests.get(url)
    response.raise_for_status()

    if input_file.exists():
        os.remove(input_file)
    with open(input_file, "wb") as f:
        f.write(response.content)

    return Path(input_file)


def get_xlsx_metadata(
    input_file: str | Path,
    sheet_name: str = constants.METADATA_SHEET_NAME.value,
    skiprows: str = constants.METADATA_SKIPROWS.value,
    nrows: int = constants.METADATA_NROWS.value,
):
    """
    Extracts metadata from an xlsx file.

    Args:
        input_file (str|Path): The path to the xlsx file.
        sheet_name (str): The name of the sheet containing the metadata. Default is constants.METADATA_SHEET_NAME.value.
        skiprows (str): The number of rows to skip before reading the data. Default is constants.METADATA_SKIPROWS.value.
        nrows (int): The number of rows to read. Default is constants.METADATA_NROWS.value.

    Returns:
        tuple: A tuple containing the source coverage dates and the last source update date.
    """
    metadata = pd.read_excel(
        Path(input_file),
        sheet_name=sheet_name,
        skiprows=skiprows,
        nrows=nrows,
        names=["metadados"],
    )

    try:
        source_coverage_dates = [
            datetime.datetime.strptime(date, "%d/%m/%y")
            for date in re.findall(
                r"(\d{2,4}/\d{2,4}/\d{2,4})", metadata["metadados"][0]
            )
        ]
        last_source_update = datetime.datetime.strptime(
            re.findall(r"(\d{2,4}/\d{2,4}/\d{2,4})", metadata["metadados"][1])[
                0
            ],
            "%d/%m/%y",
        )
    except Exception as e:
        log(
            f"Error parsing dates with format %d/%m/%y: {e}. Trying with format %d/%m/%Y."
        )
        try:
            source_coverage_dates = [
                datetime.datetime.strptime(date, "%d/%m/%Y")
                for date in re.findall(
                    r"(\d{2,4}/\d{2,4}/\d{2,4})", metadata["metadados"][0]
                )
            ]
            last_source_update = datetime.datetime.strptime(
                re.findall(
                    r"(\d{2,4}/\d{2,4}/\d{2,4})", metadata["metadados"][1]
                )[0],
                "%d/%m/%Y",
            )
        except Exception as e:
            log(
                f"Error parsing dates with format %d/%m/%Y: {e}. Returning raw date strings."
            )
            source_coverage_dates = re.findall(
                r"(\d{2,4}/\d{2,4}/\d{2,4})", metadata["metadados"][0]
            )
            last_source_update = re.findall(
                r"(\d{2,4}/\d{2,4}/\d{2,4})", metadata["metadados"][1]
            )[0]
    return source_coverage_dates, last_source_update
