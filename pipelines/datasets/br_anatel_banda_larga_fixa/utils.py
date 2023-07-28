# -*- coding: utf-8 -*-
"""
General purpose functions for the br_anatel_banda_larga_fixa
"""
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path


def download_and_unzip(url, path):
    """download and unzip a zip file

    Args:
        url (str): a url


    Returns:
        list: unziped files in a given folder
    """

    os.system(f"mkdir -p {path}")

    http_response = urlopen(url)
    zipfile = ZipFile(BytesIO(http_response.read()))
    zipfile.extractall(path=path)

    return path


def check_and_create_column(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """
    # ! Verifique se existe uma coluna em um Pandas DataFrame. Caso contrário, crie uma nova coluna com o nome fornecido
    # ! e preenchê-lo com valores NaN. Se existir, não faça nada.

    # * Parâmetros:
    # ! df (Pandas DataFrame): O DataFrame a ser verificado.
    # ! col_name (str): O nome da coluna a ser verificada ou criada.

    # * Retorna:
    # ! Pandas DataFrame: O DataFrame modificado.
    """

    if col_name not in df.columns:
        df[col_name] = ""
    return df


def to_partitions_microdados(data: pd.DataFrame, partition_columns: list[str], savepath: str):
    """Save data in to hive patitions schema, given a dataframe and a list of partition columns.
    Args:
        data (pandas.core.frame.DataFrame): Dataframe to be partitioned.
        partition_columns (list): List of columns to be used as partitions.
        savepath (str, pathlib.PosixPath): folder path to save the partitions
    Exemple:
        data = {
            "ano": [2020, 2021, 2020, 2021, 2020, 2021, 2021,2025],
            "mes": [1, 2, 3, 4, 5, 6, 6,9],
            "sigla_uf": ["SP", "SP", "RJ", "RJ", "PR", "PR", "PR","PR"],
            "dado": ["a", "b", "c", "d", "e", "f", "g",'h'],
        }
        to_partitions(
            data=pd.DataFrame(data),
            partition_columns=['ano','mes','sigla_uf'],
            savepath='partitions/'
        )
    """

    if isinstance(data, (pd.core.frame.DataFrame)):
        savepath = Path(savepath)

        # create unique combinations between partition columns
        unique_combinations = (
            data[partition_columns]
            .drop_duplicates(subset=partition_columns)
            .to_dict(orient="records")
        )

        for filter_combination in unique_combinations:
            patitions_values = [
                f"{partition}={value}"
                for partition, value in filter_combination.items()
            ]

            # get filtered data
            df_filter = data.loc[
                data[filter_combination.keys()]
                .isin(filter_combination.values())
                .all(axis=1),
                :,
            ]
            df_filter = df_filter.drop(columns=partition_columns)

            # create folder tree
            filter_save_path = Path(savepath / "/".join(patitions_values))
            filter_save_path.mkdir(parents=True, exist_ok=True)
            file_filter_save_path = Path(filter_save_path) / "data.csv"

            # append data to csv
            df_filter.to_csv(
                file_filter_save_path,
                index=False,
                mode="a",
                header=not file_filter_save_path.exists(),
            )
    else:
        raise BaseException("Data need to be a pandas DataFrame")
