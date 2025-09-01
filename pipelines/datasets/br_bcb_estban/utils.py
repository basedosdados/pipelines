# -*- coding: utf-8 -*-
"""
General purpose functions for the br_bcb_estban project
"""

import datetime as dt
import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests

from pipelines.datasets.br_bcb_estban.constants import (
    constants as br_bcb_estban_constants,
)
from pipelines.utils.utils import log


# ==== Data download functions ==== #
def fetch_bcb_documents(
    url: str, headers: dict, params: dict
) -> dict[str, Any] | None:
    """
    Fetch metadata from documents provided by Banco Central API.

    Args:
        url (str): API endpoint URL.
        headers (dict): Request headers.
        params (dict): Query parameters.

    Returns:
        dict[str, Any] | None: API response as a dictionary if successful,
        otherwise None.
    """
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        log(f"Error fetching data from BCB API: {e}")
        return None


def sort_documents_by_date(docs_metadata: dict) -> list[dict] | None:
    documents = docs_metadata.get("conteudo", [])
    if not documents:
        log("No documents found in the JSON.")
    else:
        # Sort by DataDocumento field (most recent first)
        documents.sort(
            key=lambda d: dt.datetime.fromisoformat(
                d["DataDocumento"].replace("Z", "")
            ),
            reverse=True,
        )
        return documents


def download_file(
    url: str,
    download_dir: Path,
    session: requests.Session = None,
    filename: str = None,
) -> Path | None:
    """
    Download a file from a given URL and save it to the specified directory.

    Args:
        url (str): File URL to download.
        download_dir (Path): Directory where the file will be saved.
        session (requests.Session, optional): Reusable requests session.
        filename (str, optional): Custom file name. Defaults to the file name from URL.

    Returns:
        Path | None: Path to the downloaded file, or None if download failed.
    """
    log(f"Downloading {url}")
    local_path = (
        Path(download_dir) / filename.lower()
        if filename is not None
        else Path(download_dir) / url.split("/")[-1].lower()
    )

    try:
        response = session.get(url) if session else requests.get(url)
        response.raise_for_status()

        with open(local_path, "wb") as f:
            f.write(response.content)

        return local_path
    except FileNotFoundError:
        log("Failed to locate directory or create file.")
    except Exception as e:
        log(f"Error downloading file: {e}")
    return None


def rename_columns(
    dataframe: pd.DataFrame, table_id: str
) -> dict[str, str] | None:
    """
    Validate and return the column rename mapping for a given table.

    Args:
        dataframe (pd.DataFrame): Input dataframe.
        table_id (str): Table identifier used to fetch rename mapping.

    Returns:
        dict[str, str] | None: Column rename mapping if valid, otherwise None.
    """
    mapping = br_bcb_estban_constants.TABLES_CONFIGS.value[table_id][
        "rename_mapping"
    ]
    for col in mapping.keys():
        if col not in dataframe.columns.to_list():
            log("Invalid column mapping provided!", "warning")
            return None
    return mapping


def pre_cleaning_for_pivot_long(
    dataframe: pd.DataFrame, table_id: str
) -> pd.DataFrame:
    """
    Pre-clean ESTBAN dataframe before pivoting.

    This function removes unnecessary columns, renames columns, and cleans CNPJ data.

    Args:
        dataframe (pd.DataFrame): ESTBAN dataframe.
        table_id (str): Table identifier used for column renaming.

    Returns:
        pd.DataFrame: Cleaned dataframe ready for pivoting.
    """
    rename_mapping = rename_columns(dataframe=dataframe, table_id=table_id)

    if rename_mapping is not None:
        dataframe = dataframe.rename(columns=rename_mapping)
    else:
        log("Could not rename columns!", "error")
        raise ValueError("Column renaming failed")
    dataframe.drop(
        columns={"MUNICIPIO", "CODMUN_IBGE", "CODMUN"},
        inplace=True,
        errors="ignore",
    )

    if "cnpj_agencia" in dataframe.columns.to_list():
        pattern = re.compile(r"'")
        dataframe["cnpj_agencia"] = (
            dataframe["cnpj_agencia"]
            .astype(str)
            .str.replace(pattern, "", regex=True)
        )
    return dataframe


def create_id_municipio(
    dataframe: pd.DataFrame, mapping: dict
) -> pd.DataFrame:
    """
    Create the `id_municipio` column using a mapping dictionary.

    Args:
        dataframe (pd.DataFrame): ESTBAN dataframe.
        mapping (dict): Dictionary mapping CODMUN to id_municipio.

    Returns:
        pd.DataFrame: Dataframe with the new id_municipio column.
    """
    dataframe["id_municipio"] = dataframe["CODMUN"].map(mapping)
    return dataframe


def wide_to_long(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Transform a wide ESTBAN dataframe into a long format through "verbete" columns pivotting.

    Args:
        dataframe (pd.DataFrame): Wide-format dataframe.

    Returns:
        pd.DataFrame: Long-format dataframe.
    """
    id_vars = [
        col for col in dataframe.columns if "verbete" not in str(col).lower()
    ]
    dataframe = dataframe.melt(
        id_vars=id_vars,
        var_name="verbete_descricao",
        value_name="valor",
    )
    return dataframe


def condicoes(base_date: int, valor: float) -> float:
    """
    Apply monetary unit standardization rules depending on the reference date.

    Args:
        base_date (int): Reference date in YYYYMM format.
        valor (float): Original monetary value.

    Returns:
        float: Standardized monetary value.
    """
    if base_date <= 198812:
        return round(valor / (1000**2 * 2750), 6)  # Cruzado
    elif 198901 <= base_date <= 199002:
        return round(valor / (1000 * 2750), 4)  # Cruzado Novo
    elif 199003 <= base_date <= 199307:
        return round(valor / (1000 * 2750), 4)  # Cruzeiro
    elif 199307 < base_date <= 199406:
        return round(valor / 2750, 2)  # Cruzeiro Real
    else:
        return round(valor, 0)  # Real


def standardize_monetary_units(
    dataframe: pd.DataFrame, date_column: str, value_column: str
) -> pd.DataFrame:
    """
    Correct monetary units from ESTBAN files.

    Args:
        dataframe (pd.DataFrame): Dataframe containing ESTBAN data.
        date_column (str): Column name with reference date in YYYYMM format.
        value_column (str): Column name with monetary values.

    Returns:
        pd.DataFrame: Dataframe with standardized monetary values in column "valor".
    """
    condicoes_vectorized = np.vectorize(condicoes)
    dataframe["valor"] = condicoes_vectorized(
        dataframe[date_column], dataframe[value_column]
    )
    return dataframe


def create_id_verbete_column(
    dataframe: pd.DataFrame, column_name: str
) -> pd.DataFrame:
    """
    Create an ID column for verbete entries by extracting digits.

    Args:
        dataframe (pd.DataFrame): Dataframe with verbete descriptions.
        column_name (str): Name of the new ID column.

    Returns:
        pd.DataFrame: Dataframe with the new column.
    """
    pattern_digits = re.compile(r"\D")
    dataframe[column_name] = dataframe["verbete_descricao"].str.replace(
        pattern_digits, "", regex=True
    )
    return dataframe


def create_month_year_columns(
    dataframe: pd.DataFrame, date_column: str
) -> pd.DataFrame:
    """
    Create separate year and month columns from a YYYYMM date column.

    Args:
        dataframe (pd.DataFrame): ESTBAN dataframe.
        date_column (str): Column with date in YYYYMM format.

    Returns:
        pd.DataFrame: Dataframe with "ano" and "mes" columns.
    """
    dataframe["ano"] = dataframe[date_column].astype(str).str[:4]
    dataframe["mes"] = dataframe[date_column].astype(str).str[4:]
    return dataframe


def order_cols(dataframe: pd.DataFrame, table_id: str) -> pd.DataFrame:
    """
    Reorder columns of the input dataframe, based on an order given by configs
    customized by table_id.

    Args:
        dataframe (pd.DataFrame): Input dataframe.
        table_id (str): Table identifier used to fetch column order.

    Returns:
        pd.DataFrame: Dataframe with ordered columns.
    """
    order = br_bcb_estban_constants.TABLES_CONFIGS.value[table_id]["order"]
    return dataframe[order]
