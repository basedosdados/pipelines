# -*- coding: utf-8 -*-
"""
General purpose functions for the br_bcb_estban project
"""
import requests
from lxml import html
import basedosdados as bd
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from os.path import join
from pathlib import Path
from pipelines.utils.utils import (
    log,
)
from pipelines.constants import constants

# ------- macro etapa 1 download de dados


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


# ------- macro etapa 2 tratamento de dados
# --- read files
def read_files(path: str) -> pd.DataFrame:
    """This function read a file from a given path

    Args:
        path (str): a path to a file

    Returns:
        pd.DataFrame: a dataframe with the file data
    """
    df = pd.read_csv(
        path,
        sep=";",
    )

    return df


def to_partitions(data: pd.DataFrame, partition_columns: list[str], savepath: str):
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
            # .astype(str)
            .drop_duplicates(subset=partition_columns).to_dict(orient="records")
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
                sep=",",
                encoding="utf-8",
                na_rep="",
                index=False,
                mode="a",
                header=not file_filter_save_path.exists(),
            )
    else:
        raise BaseException("Data need to be a pandas DataFrame")


def partition_data(df: pd.DataFrame, column_name: list[str], output_directory: str):
    """
    Particiona os dados em subconjuntos de acordo com os valores únicos de uma coluna.
    Salva cada subconjunto em um arquivo CSV separado.
    df: DataFrame a ser particionado
    column_name: nome da coluna a ser usada para particionar os dados
    output_directory: diretório onde os arquivos CSV serão salvos
    """

    unique_values = df[column_name].unique()

    for value in unique_values:
        value_str = str(value)
        date_value = datetime.strptime(value_str, "%Y-%m-%d %H:%M:%S").date()
        formatted_value = date_value.strftime("%Y-%m-%d")

        partition_path = os.path.join(
            output_directory, f"{column_name}={formatted_value}"
        )

        if not os.path.exists(partition_path):
            os.makedirs(partition_path)

        df_partition = df[df[column_name] == value].copy()
        df_partition.drop([column_name], axis=1, inplace=True)

        csv_path = os.path.join(partition_path, "data.csv")
        df_partition.to_csv(csv_path, index=False, encoding="utf-8", na_rep="")
