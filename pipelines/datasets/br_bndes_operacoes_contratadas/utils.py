import datetime
import os
import re
from pathlib import Path

import basedosdados as bd
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
            datetime.datetime.strptime(date, "%d/%m/%Y")
            for date in re.findall(
                r"(\d{2,4}/\d{2,4}/\d{2,4})", metadata["metadados"][0]
            )
        ]
        last_source_update = datetime.datetime.strptime(
            re.findall(r"(\d{2,4}/\d{2,4}/\d{2,4})", metadata["metadados"][1])[
                0
            ],
            "%d/%m/%Y",
        )
    except Exception as e:
        log(
            f"Error parsing dates with format %d/%m/%y: {e}. Trying with format %d/%m/%Y."
        )
        try:
            source_coverage_dates = [
                datetime.datetime.strptime(date, "%d/%m/%y")
                for date in re.findall(
                    r"(\d{2,4}/\d{2,4}/\d{2,4})", metadata["metadados"][0]
                )
            ]
            last_source_update = datetime.datetime.strptime(
                re.findall(
                    r"(\d{2,4}/\d{2,4}/\d{2,4})", metadata["metadados"][1]
                )[0],
                "%d/%m/%y",
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


def get_cnaes_by_limits_and_lists(
    dataframe: pd.DataFrame, cnae_column: str
) -> tuple[pd.DataFrame, pd.DataFrame]:

    # Cnaes by limits and by lists
    df_cnaes = dataframe.copy()
    df_cnaes[["limite_inferior", "limite_superior"]] = (
        df_cnaes[cnae_column].str.split(r"\(restante\)| a ").apply(pd.Series)
    )
    df_cnae_limites = df_cnaes.loc[df_cnaes["limite_superior"].notna()].copy()
    df_cnae_listas = df_cnaes.loc[df_cnaes["limite_superior"].isna()].copy()
    return df_cnae_limites, df_cnae_listas


def extract_cnaes_sections_by_lists(
    dataframe: pd.DataFrame, cnaes_column: str
) -> pd.DataFrame:
    # CNAEs by lists transformation:
    # split cnaes list into rows, extract section and convert to int
    df_cnae_listas = dataframe.copy()
    df_cnae_listas[["limite_inferior", "limite_superior"]] = None
    df_cnae_listas[cnaes_column] = df_cnae_listas[cnaes_column].str.split(
        r"[\n\s]*e{1}[\n\s]*|[\n\s]*,{1}[\n\s]*"
    )
    df_cnae_listas = df_cnae_listas.explode(cnaes_column)
    df_cnae_listas["secao_cnae"] = df_cnae_listas[cnaes_column].str.extract(
        r"(^[A-Z]{1})"
    )
    df_cnae_listas[cnaes_column] = (
        df_cnae_listas[cnaes_column]
        .str.extract(r"(\d+)")
        .fillna("-1")
        .astype("Int64")
    )
    return df_cnae_listas


def extract_cnaes_sections_by_limits(
    dataframe: pd.DataFrame, df_diretorios: pd.DataFrame, cnaes_column: str
) -> pd.DataFrame:
    # CNAEs by limits transformation:
    # create cnaes list from limits, extract section and convert to int
    df_cnae_limites = dataframe.copy()
    df_cnae_limites[cnaes_column] = None
    # Eztract section
    df_cnae_limites["secao_cnae"] = df_cnae_limites[
        "limite_inferior"
    ].str.extract(r"(^[A-Z]{1})")
    # Extract limits numbers
    df_cnae_limites["limite_inferior"] = (
        df_cnae_limites["limite_inferior"]
        .str.extract(r"(\d+)")
        .fillna("-1")
        .astype("int")
    )
    df_cnae_limites["limite_superior"] = (
        df_cnae_limites["limite_superior"]
        .str.extract(r"(\d+)")
        .fillna("-1")
        .astype("int")
    )
    df_cnae_limites.loc[
        df_cnae_limites["limite_superior"] == -1, "limite_superior"
    ] = None
    df_cnae_limites.loc[
        df_cnae_limites["limite_inferior"] == -1, "limite_inferior"
    ] = None
    df_cnae_limites["limite_inferior"] = df_cnae_limites[
        "limite_inferior"
    ].astype("Int64")
    df_cnae_limites["limite_superior"] = df_cnae_limites[
        "limite_superior"
    ].astype("Int64")

    # For rows with null upper limit, fill the upper limit with the max division of the section
    df_cnae_limites.loc[
        df_cnae_limites["limite_superior"].isna(), "limite_superior"
    ] = df_cnae_limites.loc[
        df_cnae_limites["limite_superior"].isna(), "secao_cnae"
    ].apply(
        lambda x: df_diretorios.loc[
            df_diretorios["secao"] == x, "divisao"
        ].max()
    )
    for i, row in df_cnae_limites.iterrows():
        df_cnae_limites.loc[i, cnaes_column] = str(
            list(range(row["limite_inferior"], row["limite_superior"] + 1))
        )
    # Explode cnaes list into rows and convert to string with leading zeros
    df_cnae_limites[cnaes_column] = df_cnae_limites[cnaes_column].apply(
        lambda x: eval(x)
    )
    df_cnae_limites = df_cnae_limites.explode(cnaes_column)
    df_cnae_limites[cnaes_column] = df_cnae_limites[cnaes_column].apply(
        lambda x: str(x).zfill(2)
    )
    return df_cnae_limites


def extract_cnae_hierarchy(
    dataframe: pd.DataFrame, cnae_column: str
) -> pd.DataFrame:
    df_extracted = dataframe.copy()
    df_extracted["secao_cnae"] = df_extracted[cnae_column].str.extract(
        r"(^[A-Z]{1})"
    )
    df_extracted["divisao_cnae"] = df_extracted[cnae_column].str.extract(
        r"^[A-Z]{1}(\d{2})"
    )
    df_extracted["grupo_cnae"] = df_extracted[cnae_column].str.extract(
        r"^[A-Z]{1}(\d{3})"
    )
    df_extracted["classe_cnae"] = df_extracted[cnae_column].str.extract(
        r"^[A-Z]{1}(\d{5})"
    )
    df_extracted["subclasse_cnae"] = df_extracted[cnae_column].str.extract(
        r"^[A-Z](\d{7})"
    )
    df_extracted.loc[
        df_extracted["grupo_cnae"].str.endswith("000"),
        ["divisao_cnae", "classe_cnae"],
    ] = None
    df_extracted = df_extracted.drop(columns=[cnae_column])
    df_diretorios = bd.read_sql(
        """
        SELECT DISTINCT secao, divisao, grupo, classe, subclasse 
        FROM `basedosdados.br_bd_diretorios_brasil.cnae_2`
        """,
        from_file=True,
    )

    for col in ["divisao", "grupo", "classe", "subclasse"]:
        df_extracted.loc[
            ~df_extracted[f"{col}_cnae"].isin(df_diretorios[col].values),
            f"{col}_cnae",
        ] = None
    return df_extracted


def check_duplicates(dataframe: pd.DataFrame, columns: list[str]):
    duplicates = dataframe.duplicated(subset=columns, keep=False)
    if duplicates.any():
        log(
            f"Encontrados {duplicates.sum()} registros duplicados com base nas colunas {columns}."
        )
        log("Exibindo os registros duplicados:")
        log(dataframe[duplicates])
    else:
        log(
            f"Nenhum registro duplicado encontrado com base nas colunas {columns}."
        )


def count_nulls(dataframe: pd.DataFrame, columns: list[str]):
    null_counts = dataframe[columns].isna().sum()
    log("Contagem de valores nulos por coluna:")
    log(null_counts)
