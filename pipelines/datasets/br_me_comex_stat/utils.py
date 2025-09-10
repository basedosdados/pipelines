# -*- coding: utf-8 -*-
"""Utils for the Brazilian Comex Stat pipeline."""

# pylint: disable=invalid-name
import os
import time as tm
from datetime import datetime
from typing import List, Literal

import pandas as pd
import wget

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
    table_type: str,
    table_name: str,
    years_download: List[str],
):
    """A simple crawler to download data from comex stat website.

    Args:
        path (str): the path to store the data
        table_type (str): the table type is either ncm or mun. ncm stands for 'nomenclatura comum do mercosul' and
        mun for 'município'.
        table_name (str): the table name is the original name of the zip file with raw data from comex stat website
    """

    for year_download in years_download:
        year = datetime.strptime(year_download, "%Y-%m").year

        log(f"Donwloading year ->>> {year}")
        table_name_urls = {
            "mun_imp": f"https://balanca.economia.gov.br/balanca/bd/comexstat-bd/{table_type}/IMP_{year}_MUN.csv",
            "mun_exp": f"https://balanca.economia.gov.br/balanca/bd/comexstat-bd/{table_type}/EXP_{year}_MUN.csv",
            "ncm_imp": f"https://balanca.economia.gov.br/balanca/bd/comexstat-bd/{table_type}/IMP_{year}.csv",
            "ncm_exp": f"https://balanca.economia.gov.br/balanca/bd/comexstat-bd/{table_type}/EXP_{year}.csv",
        }

        # Selects a url given a table name
        url = table_name_urls[table_name]

        log(f"Downloading {url}")

        # Downloads the file and saves it
        wget.download(url, out=path + table_name + "/input")

        # Sleep for 8 secs in between iterations
        tm.sleep(8)


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
    url = f"{base_url_validation}/{str(table_type)}/{str(trade_type).upper()}_TOTAIS_CONFERENCIA{suffix}.csv"
    wget.download(url, out=output_path)
    return output_path


def validate_table(
    filename: str,
    dataframe: pd.DataFrame,
    table_type: Literal["mun", "ncm"],
    path: str,
):
    """
    Validates a transformed dataframe against official validation files.
    It performs consistency checks between the processed dataframe
    and external validation datasets provided by MDIC. The validation ensures that:
    - The aggregated sums (FOB value, net weight, statistical quantity) match
      the validation file for the reference year.
    - The number of rows matches the validation file.

    Args:
        filename (str): Name of the input file to determine trade type (import/export).
        dataframe (pd.DataFrame): Transformed dataframe to be validated.
        table_type {"mun", "ncm"}: Indicates which validation table to use.
    """

    # Infer trade type from filename
    if "imp" in str(filename).lower():
        trade_type = "imp"
    elif "exp" in str(filename).lower():
        trade_type = "exp"
    else:
        raise ValueError("Invalid trade_type detected in filename.")

    # Download and prepare validation files
    download_validation(
        path, table_type, trade_type, comex_constants.VALIDATION_LINK.value
    )

    table_name = "validation"
    validation_dir = f"{path}{table_name}/input/"
    validation_file_list = os.listdir(validation_dir)

    for validation_file in validation_file_list:
        if (
            table_type in str(validation_file).lower()
            and trade_type in str(validation_file).lower()
        ):
            df_validation = pd.read_csv(
                f"{validation_dir}{validation_file}", sep=";"
            )
            log(f"Using validation file: {validation_file}", "info")

            # Rename columns to standardized schema
            rename_validation = {
                "ARQUIVO": "nome_arquivo",
                "CO_ANO": "ano",
                "QT_ESTAT": "quantidade_estatistica",
                "KG_LIQUIDO": "peso_liquido_kg",
                "VL_FOB": "valor_fob_dolar",
                "NUMERO_LINHAS": "linhas",
            }
            df_validation.rename(columns=rename_validation, inplace=True)

            # Ensure the dataframe has only one reference year
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

                # Aggregate dataframe values for comparison
                df_grouped = dataframe[cols_to_evaluate].sum().to_frame().T
                df_grouped.reset_index(drop=True, inplace=True)

                # Extract validation totals for the same year
                df_val = df_validation.loc[
                    df_validation["ano"] == ano, cols_to_evaluate
                ].reset_index(drop=True)

                # Column-by-column validation
                for col in cols_to_evaluate:
                    wrangled_value = df_grouped.at[0, col]
                    validation_value = df_val.at[0, col]

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

                # Validate row counts
                validation_rows = int(
                    df_validation.loc[
                        df_validation["ano"] == ano, "linhas"
                    ].values[0]
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
