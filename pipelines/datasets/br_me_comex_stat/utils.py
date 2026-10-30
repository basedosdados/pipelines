"""Utils for the Brazilian Comex Stat pipeline."""

import os
import re
import time as tm
from datetime import datetime
from pathlib import Path
from typing import Literal

import pandas as pd
import requests

from pipelines.datasets.br_me_comex_stat.constants import (
    constants as comex_constants,
)
from pipelines.utils.utils import log


def create_paths(
    path: str,
    table_name: str,
):
    """this function creates temporary directories to store input and output files

    Args:
        path (str): a standard directory to store input and output files from all flows
        table_name (str): the name of the table to compose the directory structure and separate input and output files
        of diferent tables

    """
    path_temps = [
        path,
        path + table_name + "/input/",
        path + table_name + "/output/",
    ]

    for path_temp in path_temps:
        os.makedirs(path_temp, exist_ok=True)


def download_data(
    path: str,
    table_name: str,
    years_download: list[str],
):
    """A simple crawler to download data from comex stat website.

    Args:
        path (str): the path to store the data
        mun for 'município'.
        table_name (str): the table name is the original name of the zip file with raw data from comex stat website
    """

    for year_download in years_download:
        year = datetime.strptime(year_download, "%Y-%m").year

        log(f"Donwloading year ->>> {year}")
        table_name_urls = {
            "mun_imp": f"https://balanca.economia.gov.br/balanca/bd/comexstat-bd/mun/IMP_{year}_MUN.csv",
            "mun_exp": f"https://balanca.economia.gov.br/balanca/bd/comexstat-bd/mun/EXP_{year}_MUN.csv",
            "ncm_imp": f"https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/IMP_{year}.csv",
            "ncm_exp": f"https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/EXP_{year}.csv",
        }

        # Selects a url given a table name
        url = table_name_urls[table_name]
        filename = url.split("/")[-1]
        log(f"Downloading {url}")

        response = requests.get(url, verify=False)
        response.raise_for_status()  # Raise an error for bad responses

        with open(path + table_name + "/input/" + filename, "wb") as f:
            f.write(response.content)

        # Sleep for 8 secs in between iterations
        tm.sleep(8)


def check_year_matches_filename(
    dataframe: pd.DataFrame, filename: str
) -> None:
    """Confere se todas as linhas têm o `ano` do nome do arquivo.

    Args:
        dataframe: Arquivo lido, com as colunas já renomeadas.
        filename: Nome do arquivo baixado, como `EXP_2026.csv` ou
            `IMP_2026_MUN.csv`.

    Raises:
        ValueError: Quando alguma linha tem outro `ano`.
    """
    file_year = int(re.findall(r"\d{4}", filename)[0])
    other_years = dataframe.loc[dataframe["ano"] != file_year, "ano"]

    if not other_years.empty:
        raise ValueError(
            f"{len(other_years)} linha(s) de {filename} com `ano` diferente "
            f"de {file_year}: {other_years.unique()[:5].tolist()}"
        )


def count_written_rows(output_dir: str | Path) -> int:
    """Conta as linhas gravadas nos `.csv` de um diretório.

    Args:
        output_dir: Raiz das partições.

    Returns:
        O total de linhas, sem os cabeçalhos.
    """
    rows = 0

    for path in sorted(Path(output_dir).rglob("*.csv")):
        chunks = pd.read_csv(
            path,
            usecols=[0],
            dtype=str,
            chunksize=comex_constants.CSV_CHUNK_ROWS.value,
        )
        for chunk in chunks:
            rows += len(chunk)

    return rows


def check_rows_written(rows_read: int, output_dir: str | Path) -> None:
    """Confere se as partições gravadas têm as mesmas linhas que as lidas.

    Args:
        rows_read: Linhas lidas da fonte.
        output_dir: Raiz das partições.

    Raises:
        ValueError: Quando as partições têm mais ou menos linhas que as lidas.
    """
    rows_written = count_written_rows(output_dir)

    if rows_written != rows_read:
        raise ValueError(
            f"{rows_read} linhas lidas e {rows_written} gravadas em "
            f"{output_dir}."
        )

    log(f"{rows_read} linhas lidas e gravadas em {output_dir}")


def download_validation(
    path: str,
    table_type: Literal["mun", "ncm"],
    trade_type: Literal["imp", "exp"],
    base_url_validation: str,
) -> str:
    if table_type == "mun":
        suffix = "_MUN"
    elif table_type == "ncm":
        suffix = ""
    else:
        raise ValueError("Invalid table_type.")

    table_name = "validation"
    create_paths(path, table_name)

    output_path = path + table_name + "/input"
    url = f"{base_url_validation}/{table_type!s}/{str(trade_type).upper()}_TOTAIS_CONFERENCIA{suffix}.csv"
    filename = url.split("/")[-1]

    response = requests.get(url, verify=False)
    response.raise_for_status()  # Raise an error for bad responses

    with open(f"{output_path}/{filename}", "wb") as f:
        f.write(response.content)
    return f"{output_path}/{filename}"


def check_totals_against_validation_file(
    filename: str,
    dataframe: pd.DataFrame,
    table_type: Literal["mun", "ncm"],
    path: str,
):
    """
    Validates a transformed dataframe against official validation files.
    It performs consistency checks between the processed dataframe
    and external validation datasets provided by MDIC. The validation ensures that:
    - The file has at least one data row.
    - The aggregated sums (FOB value, net weight, statistical quantity) match
      the validation file for the reference year.
    - The number of rows matches the validation file.

    Args:
        filename (str): Name of the input file to determine trade type (import/export).
        dataframe (pd.DataFrame): Transformed dataframe to be validated.
        table_type {"mun", "ncm"}: Indicates which validation table to use.
    """

    if dataframe.empty:
        raise ValueError(f"Validation failed: {filename} has no data rows.")

    # Infer trade type from filename
    # pyrefly: ignore [unnecessary-type-conversion]
    if "imp" in str(filename).lower():
        trade_type = "imp"
    # pyrefly: ignore [unnecessary-type-conversion]
    elif "exp" in str(filename).lower():
        trade_type = "exp"
    else:
        raise ValueError("Invalid trade_type detected in filename.")

    validation_path = download_validation(
        path, table_type, trade_type, comex_constants.VALIDATION_LINK.value
    )
    df_validation = pd.read_csv(validation_path, sep=";")
    log(f"Using validation file: {validation_path}", "info")

    rename_validation = {
        "ARQUIVO": "nome_arquivo",
        "CO_ANO": "ano",
        "QT_ESTAT": "quantidade_estatistica",
        "KG_LIQUIDO": "peso_liquido_kg",
        "VL_FOB": "valor_fob_dolar",
        "NUMERO_LINHAS": "linhas",
    }
    df_validation = df_validation.rename(columns=rename_validation)

    if len(dataframe["ano"].unique()) == 1:
        ano = dataframe["ano"].unique()[0]
        log(f"Unique reference year detected: {ano}", "info")

        cols_to_evaluate = [
            col
            for col in [
                "peso_liquido_kg",
                "valor_fob_dolar",
                "quantidade_estatistica",
            ]
            if col in dataframe
        ]

        df_grouped = dataframe[cols_to_evaluate].sum().to_frame().T
        df_grouped = df_grouped.reset_index(drop=True)

        df_val = df_validation.loc[
            df_validation["ano"] == ano, cols_to_evaluate
        ].reset_index(drop=True)

        for col in cols_to_evaluate:
            wrangled_value = df_grouped.loc[0, col]
            validation_value = df_val.loc[0, col]

            if wrangled_value == validation_value:
                log(
                    f"VALIDATION: Sum for '{col}' matches between dataframe and validation file for year {ano}.",
                    "info",
                )
            else:
                log(
                    f"VALIDATION: Sum mismatch for '{col}': dataframe={wrangled_value}, validation={validation_value} (year {ano}).",
                    "warning",
                )
                raise ValueError(
                    f"Validation failed: mismatch in column '{col}' for year {ano}."
                )

        validation_rows = int(
            df_validation.loc[
                df_validation["ano"] == ano, "linhas"
            ].to_numpy()[0]
        )
        dataframe_rows = len(dataframe)

        if validation_rows == dataframe_rows:
            log(
                f"VALIDATION: Row count matches: dataframe={dataframe_rows}, validation={validation_rows}.",
                level="info",
            )
        else:
            log(
                f"VALIDATION: Row count mismatch: dataframe={dataframe_rows}, validation={validation_rows}.",
                level="warning",
            )
            raise ValueError(
                f"Validation failed: row count mismatch for year {ano}."
            )
