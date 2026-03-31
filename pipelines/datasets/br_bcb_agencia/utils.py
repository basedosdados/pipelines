"""
General purpose functions for the br_bcb_agencia project
"""

import datetime as dt
import re
import unicodedata
from pathlib import Path
from typing import Any

import pandas as pd
import requests

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
    """
    Sort a list of documents by their publication date in descending order.

    Args:
        docs_metadata (dict): Metadata containing a "conteudo" field
            with a list of documents.

    Returns:
        list[dict] | None: Documents sorted by "DataDocumento" (most recent first),
        or None if no documents found.
    """
    documents = docs_metadata.get("conteudo", [])
    if not documents:
        log("No documents found in the JSON.")
        return None

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
    filename: str | None = None,
) -> Path | None:
    """
    Downloads a file from the specified URL and saves it to a given directory.

    Args:
        url (str): The URL of the file to download.
        download_dir (Path): Directory where the file should be saved.
        session (requests.Session, optional): Optional requests session for reusing connections.
        filename (str, optional): Custom name to save the file as. Defaults to the name in the URL.

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


# ==== File reading and initial processing functions ==== #
def find_cnpj_row_number(file_path: str) -> int:
    """
    Finds the row number where the first occurrence of the value 'CNPJ'
    (always in the first column) appears and returns the row number.

    Args:
        file_path (str): File path.

    Returns:
        int: Row number corresponding to the header.
    """
    dataframe = pd.read_excel(file_path, nrows=20)
    first_col = dataframe.columns[0]
    log(f"First column identified: {first_col}")

    dataframe[first_col] = dataframe[first_col].str.strip()
    match = dataframe[dataframe[first_col] == "CNPJ"]

    if not match.empty:  # noqa: SIM108
        cnpj_row_number = match.index.tolist()[0]
    else:
        cnpj_row_number = 9  # default value when not found

    return cnpj_row_number


def get_conv_names(file_path: str, skiprows: int) -> dict:
    """
    Gets column names from a file for use as converters for column names
    to the reading method (string).

    Args:
        file_path (str): File path.
        skiprows (int): Number of rows to skip.

    Returns:
        dict: Dictionary mapping column names to `str`.
    """
    dataframe = pd.read_excel(file_path, nrows=20, skiprows=skiprows)
    cols = dataframe.columns
    conv = dict(zip(cols, [str] * len(cols), strict=False))
    return conv


def read_file(file_path: str, file_name: str) -> pd.DataFrame:
    """
    Reads an Excel file of agencies and returns a processed DataFrame.

    Args:
        file_path (str): File path.
        file_name (str): File name, used to extract year and month.

    Returns:
        pd.DataFrame: DataFrame containing agency data.
    """
    try:
        skiprows = find_cnpj_row_number(file_path=file_path) + 1
        conv = get_conv_names(file_path=file_path, skiprows=skiprows)
        log("Column converters generated.")
        dataframe = pd.read_excel(
            file_path, skiprows=skiprows, converters=conv, skipfooter=2
        )
        dataframe = create_year_month_cols(dataframe=dataframe, file=file_name)

    except Exception as e:
        log(
            f"Error capturing header row (skiprows): {e}. Using default value (9)."
        )
        conv = get_conv_names(file_path=file_path, skiprows=9)
        dataframe = pd.read_excel(
            file_path, skiprows=9, converters=conv, skipfooter=2
        )
        dataframe = create_year_month_cols(dataframe=dataframe, file=file_name)

    return dataframe


def clean_column_names(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes and cleans DataFrame column names.

    Operations applied:
        - Removes leading and trailing whitespace.
        - Removes accents.
        - Replaces special characters with "_".
        - Converts to lowercase.

    Args:
        dataframe (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with cleaned column names.
    """
    log("Cleaning column names...")
    dataframe.columns = dataframe.columns.str.strip()
    dataframe.columns = dataframe.columns.map(
        lambda x: (
            unicodedata.normalize("NFKD", str(x))
            .encode("ascii", "ignore")
            .decode("utf-8")
        )
    )
    dataframe.columns = dataframe.columns.str.replace(
        r"[^\w\s]+", "_", regex=True
    )
    dataframe.columns = dataframe.columns.str.lower()
    return dataframe


# ==== Transformation and standardization functions ==== #
def create_year_month_cols(dataframe: pd.DataFrame, file: str) -> pd.DataFrame:
    """
    Creates year and month columns from the file name.

    Args:
        dataframe (pd.DataFrame): Input DataFrame.
        file (str): File name (first 6 digits are year and month).

    Returns:
        pd.DataFrame: DataFrame with 'ano' and 'mes' columns added.
    """
    dataframe["ano"] = file[0:4]
    dataframe["mes"] = file[4:6]
    log(f"Columns 'ano'={file[0:4]} and 'mes'={file[4:6]} created.")
    return dataframe


def check_and_create_column(
    dataframe: pd.DataFrame, col_name: str
) -> pd.DataFrame:
    """
    Checks if a column exists in the DataFrame.
    If not, creates the column filled with empty strings.

    Args:
        dataframe (pd.DataFrame): Input DataFrame.
        col_name (str): Column name to check/create.

    Returns:
        pd.DataFrame: Updated DataFrame.
    """
    if col_name not in dataframe.columns:
        dataframe[col_name] = ""
        log(f"Column '{col_name}' created (did not exist in DataFrame).")
    return dataframe


def rename_cols() -> dict:
    """
    Dictionary for standardizing column names.

    Returns:
        dict: Dictionary mapping old names to new names.
    """
    rename_dict = {
        "cnpj": "cnpj",
        "sequencial do cnpj": "sequencial_cnpj",
        "dv do cnpj": "dv_do_cnpj",
        "nome instituicao": "instituicao",
        "nome da instituicao": "instituicao",
        "segmento": "segmento",
        "segmentos": "segmento",
        "cod compe ag": "id_compe_bcb_agencia",
        "cod compe bco": "id_compe_bcb_instituicao",
        "nome da agencia": "nome_agencia",
        "nome agencia": "nome_agencia",
        "endereco": "endereco",
        "numero": "numero",
        "complemento": "complemento",
        "bairro": "bairro",
        "cep": "cep",
        "municipio": "nome",
        "estado": "sigla_uf",
        "uf": "sigla_uf",
        "data inicio": "data_inicio",
        "ddd": "ddd",
        "fone": "fone",
        "id instalacao": "id_instalacao",
        "municipio ibge": "id_municipio",
    }
    return rename_dict


def order_cols() -> list:
    """
    Default column order for the DataFrame.

    Returns:
        list: Ordered list of column names.
    """
    return [
        "ano",
        "mes",
        "sigla_uf",
        "nome",
        "id_municipio",
        "data_inicio",
        "cnpj",
        "nome_agencia",
        "instituicao",
        "segmento",
        "id_compe_bcb_agencia",
        "id_compe_bcb_instituicao",
        "cep",
        "endereco",
        "complemento",
        "bairro",
        "ddd",
        "fone",
        "id_instalacao",
    ]


def clean_nome_municipio(
    dataframe: pd.DataFrame, col_name: str
) -> pd.DataFrame:
    """
    Cleans the municipality column.

    Operations applied:
        - Removes accents.
        - Removes special characters.
        - Converts to lowercase.
        - Removes extra spaces.

    Args:
        dataframe (pd.DataFrame): Input DataFrame.
        col_name (str): Municipality column name.

    Returns:
        pd.DataFrame: Updated DataFrame.
    """
    dataframe[col_name] = dataframe[col_name].apply(
        lambda x: (
            unicodedata.normalize("NFKD", str(x))
            .encode("ascii", "ignore")
            .decode("utf-8")
        )
    )
    dataframe[col_name] = dataframe[col_name].apply(
        lambda x: re.sub(r"[^\w\s]", "", x)
    )
    dataframe[col_name] = dataframe[col_name].str.lower().str.strip()
    return dataframe


def remove_latin1_accents_from_df(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Removes accents from all values in a DataFrame.

    Args:
        dataframe (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: Updated DataFrame.
    """
    for col in dataframe.columns:
        dataframe[col] = dataframe[col].apply(
            lambda x: "".join(
                c
                for c in unicodedata.normalize("NFD", str(x))
                if unicodedata.category(c) != "Mn"
            )
        )
    return dataframe


def remove_non_numeric_chars(s: str) -> str:
    """
    Removes non-numeric characters from a string.

    Args:
        s (str): Input string.

    Returns:
        str: String containing only numbers.
    """
    return re.sub(r"\D", "", s)


def remove_empty_spaces(s: str) -> str:
    """
    Removes whitespace from a string.

    Args:
        s (str): Input string.

    Returns:
        str: String without spaces.
    """
    return re.sub(r"\s", "", s)


def create_cnpj_col(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Builds the `cnpj` column by concatenating its parts.

    Args:
        dataframe (pd.DataFrame): DataFrame containing columns
            `cnpj`, `sequencial_cnpj` and `dv_do_cnpj`.

    Returns:
        pd.DataFrame: DataFrame with the final `cnpj` column.
    """
    dataframe["sequencial_cnpj"] = dataframe["sequencial_cnpj"].astype(str)
    dataframe["dv_do_cnpj"] = dataframe["dv_do_cnpj"].astype(str)
    dataframe["cnpj"] = dataframe["cnpj"].astype(str)

    dataframe["cnpj"] = (
        dataframe["cnpj"]
        + dataframe["sequencial_cnpj"]
        + dataframe["dv_do_cnpj"]
    )
    log("Column 'cnpj' built.")
    return dataframe


def str_to_title(dataframe: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """
    Converts the values of a column to title case.
    """
    dataframe[column_name] = dataframe[column_name].str.title()
    return dataframe


def strip_dataframe_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Removes extra spaces from all string values in a DataFrame.

    Args:
        dataframe (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: Updated DataFrame.
    """
    for col in dataframe.select_dtypes(include=["object", "string"]).columns:
        dataframe[col] = dataframe[col].str.strip()
    return dataframe


def format_date(date_str: str) -> str:
    """
    Converts a date string to the standard format YYYY-MM-DD.

    If conversion is not possible, returns an empty string.

    Args:
        date_str (str): Date in variable format.

    Returns:
        str: Formatted date or empty string.
    """
    try:
        date_obj = pd.to_datetime(date_str)
        return date_obj.strftime("%Y-%m-%d")
    except ValueError:
        return ""
