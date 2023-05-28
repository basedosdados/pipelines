# -*- coding: utf-8 -*-
"""
Tasks for br_ons_avaliacao_operacao
"""
import os
import pandas as pd

from prefect import task
from pipelines.utils.utils import (
    log,
)
from pipelines.datasets.br_ons_estimativa_custos.constants import constants
from pipelines.datasets.br_ons_estimativa_custos.utils import (
    create_paths,
    crawler_ons,
    download_data as dw,
    change_columns_name,
    remove_latin1_accents_from_df,
    order_df,
    process_date_column,
    process_datetime_column,
)


@task
def download_data(
    table_name: str,
):
    """
    This task crawls the ons website to extract all download links for the given table name.
    Args:
        table_name (str): the table name to be downloaded
    Returns:
        list: a list of file links
    """
    # create paths
    create_paths(
        path=constants.PATH.value,
        # acessa link
        table_name=table_name,
    )
    print("paths created")

    url_list = crawler_ons(
        # acessa valor
        url=constants.TABLE_NAME_URL_DICT.value[table_name],
    )
    print("urls fetched")

    dw(
        path=constants.PATH.value,
        url_list=url_list,
        table_name=table_name,
    )
    print("data downloaded")


@task
def wrang_data(
    table_name: str,
) -> pd.DataFrame:
    path_input = f"C:/tmp/br_ons/{table_name}/input"
    path_output = f"C:/tmp/br_ons/{table_name}/output"

    df_list = []

    for file in os.listdir(path_input):
        if table_name == "custo_marginal_operacao_semanal":
            print(f"fazendo {file}")
            file = path_input + "/" + file

            df = pd.read_csv(
                file,
                sep=";",
                # encoding = 'latin1',
                decimal=",",
                thousands=".",
            )

            print("fazendo file")

            architecture_link = constants.TABLE_NAME_ARCHITECHTURE_DICT.value[
                table_name
            ]

            # rename cols
            df = change_columns_name(url=architecture_link, df=df)

            df["data"] = pd.to_datetime(df["data"]).dt.date

            df = remove_latin1_accents_from_df(df)

            df_list.append(df)

            del df

        else:
            print(f"fazendo {file}")
            file = path_input + "/" + file

            df = pd.read_csv(
                file,
                sep=";",
                # encoding = 'latin1',
                decimal=",",
                thousands=".",
            )

            print("fazendo file")

            # rename cols
            df = change_columns_name(url=architecture_link, df=df)

            df = process_datetime_column(
                df=df,
                datetime_column="data",
            )

            print(df["data"].head(5))
            print("datas formatadas")

            architecture_link = constants.TABLE_NAME_ARCHITECHTURE_DICT.value[
                table_name
            ]

            # acusa erro aqui
            # df = remove_latin1_accents_from_df(df)

            df_list.append(df)

            del df

    df = pd.concat(df_list)

    df = order_df(url=architecture_link, df=df)

    df.to_csv(
        path_output + "/" + f"{table_name}.csv",
        sep=",",
        index=False,
        na_rep="",
        encoding="utf-8",
    )

    del df

    return path_output
