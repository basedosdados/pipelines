# -*- coding: utf-8 -*-
import os
from pathlib import Path
from typing import List

import pandas as pd

file_directory = os.path.dirname(__file__)


def read_data(file_dir, separator=";"):
    data_directory = os.path.join(file_directory, file_dir)

    return pd.read_csv(data_directory, sep=separator)


def remove_empty_rows(df):
    return df.dropna(axis=0, how="all")


def remove_empty_columns(df):
    return df.drop(list(df.filter(regex="Unnamed")), axis=1)


def replace_commas(value):
    string_value = str(value)
    num_commas = string_value.count(",")
    if num_commas == 1:
        return string_value.replace(",", ".")
    elif num_commas > 1:
        return string_value.replace(",", "", num_commas - 1).replace(",", ".")
    else:
        return string_value


def remove_dots(value):
    string_value = str(value)
    num_dots = string_value.count(".")
    if num_dots > 1:
        return string_value.replace(".", "", num_dots - 1)
    else:
        return string_value


def get_month_number(month_column):
    month_lower = month_column.str.lower()
    month_inits = month_lower.str[:3]

    month_numbers = {
        "jan": "1",
        "fev": "2",
        "mar": "3",
        "abr": "4",
        "mai": "5",
        "jun": "6",
        "jul": "7",
        "ago": "8",
        "set": "9",
        "out": "10",
        "nov": "11",
        "dez": "12",
    }
    return month_inits.replace(month_numbers).astype("int")


def get_state_letters(state_column):
    state_lower = state_column.str.lower()
    states = {
        "acre": "AC",
        "alagoas": "AL",
        "amapá": "AP",
        "amazonas": "AM",
        "bahia": "BA",
        "ceará": "CE",
        "distrito federal": "DF",
        "espírito santo": "ES",
        "goiás": "GO",
        "maranhão": "MA",
        "mato grosso": "MT",
        "mato grosso do sul": "MS",
        "minas gerais": "MG",
        "pará": "PA",
        "paraíba": "PB",
        "paraná": "PR",
        "pernambuco": "PE",
        "piauí": "PI",
        "rio de janeiro": "RJ",
        "rio grande do norte": "RN",
        "rio grande do sul": "RS",
        "rondônia": "RO",
        "roraima": "RR",
        "santa catarina": "SC",
        "são paulo": "SP",
        "sergipe": "SE",
        "tocantins": "TO",
    }
    return state_lower.replace(states)


def get_region_letters(region_names):
    region_lower = region_names.str.lower()

    regions = {
        "norte": "N",
        "sul": "S",
        "centro-oeste": "CO",
        "sudeste": "SE",
        "nordeste": "NE",
    }
    return region_lower.replace(regions)


def to_partitions(
    data: pd.DataFrame,
    partition_columns: List[str],
    savepath: str,
    file_type: str = "csv",
):
    """Save data in to hive patitions schema, given a dataframe and a list of partition columns.
    Args:
        data (pandas.core.frame.DataFrame): Dataframe to be partitioned.
        partition_columns (list): List of columns to be used as partitions.
        savepath (str, pathlib.PosixPath): folder path to save the partitions.
        file_type (str): default to csv. Accepts parquet.
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
            savepath='partitions/',
        )
    """

    if isinstance(data, (pd.core.frame.DataFrame)):
        savepath = Path(savepath)
        # create unique combinations between partition columns
        unique_combinations = (
            data[partition_columns]
            # .astype(str)
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

            if file_type == "csv":
                # append data to csv
                file_filter_save_path = Path(filter_save_path) / "data.csv"
                df_filter.to_csv(
                    file_filter_save_path,
                    sep=",",
                    encoding="utf-8",
                    na_rep="",
                    index=False,
                    mode="a",
                    header=not file_filter_save_path.exists(),
                )
            elif file_type == "parquet":
                # append data to parquet
                file_filter_save_path = Path(filter_save_path) / "data.parquet"
                df_filter.to_parquet(
                    file_filter_save_path, index=False, compression="gzip"
                )
    else:
        raise BaseException("Data need to be a pandas DataFrame")


def save_data(df, file_dir, partition_cols):
    data_directory = os.path.join(file_directory, file_dir)
    to_partitions(
        data=df, partition_columns=partition_cols, savepath=data_directory
    )
