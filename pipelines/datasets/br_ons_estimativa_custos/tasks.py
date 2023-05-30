# -*- coding: utf-8 -*-
"""
Tasks for br_ons_avaliacao_operacao
"""
import os
import pandas as pd
import time as tm
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
    log("paths created")

    url_list = crawler_ons(
        # acessa valor
        url=constants.TABLE_NAME_URL_DICT.value[table_name],
    )
    log("urls fetched")
    tm.sleep(2)
    dw(
        path=constants.PATH.value,
        url_list=url_list,
        table_name=table_name,
    )
    log("data downloaded")
    tm.sleep(2)


@task
def wrang_data(
    table_name: str,
) -> pd.DataFrame:
    path_input = f"/tmp/br_ons_estimativa_custos/{table_name}/input"
    path_output = f"/tmp/br_ons_estimativa_custos/{table_name}/output"

    df_list = []

    for file in os.listdir(path_input):
        if table_name == "custo_marginal_operacao_semanal":
            log(f"fazendo {file}")
            file = path_input + "/" + file
            log(f"{file}")
            df = pd.read_csv(
                file,
                sep=";",
                # encoding = 'latin1',
                decimal=",",
                thousands=".",
            )

            architecture_link = constants.TABLE_NAME_ARCHITECHTURE_DICT.value[
                table_name
            ]
            log("renaming cols")
            # rename cols
            df = change_columns_name(url=architecture_link, df=df)

            df["data"] = pd.to_datetime(df["data"]).dt.date

            log("removing accents")
            df = remove_latin1_accents_from_df(df)

            df_list.append(df)

            del df

        if table_name == "balanco_energia_subsistemas":
            file = path_input + "/" + file

            df = pd.read_csv(
                file,
                sep=";",
                # encoding = 'latin1',
                decimal=",",
                thousands=".",
            )

            log("fazendo file")
            architecture_link = constants.TABLE_NAME_ARCHITECHTURE_DICT.value[
                table_name
            ]
            # rename cols
            df = change_columns_name(url=architecture_link, df=df)

            df = process_datetime_column(
                df=df,
                datetime_column="data",
            )

            df.rename(columns={"id_subsistena": "id_subsistema"}, inplace=True)

            log("datas formatadas")

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

            log("fazendo file")
            architecture_link = constants.TABLE_NAME_ARCHITECHTURE_DICT.value[
                table_name
            ]
            # rename cols
            df = change_columns_name(url=architecture_link, df=df)

            df = process_datetime_column(
                df=df,
                datetime_column="data",
            )

            log(df["data"].head(5))
            log("datas formatadas")

            df_list.append(df)

            del df

    df = pd.concat(df_list)
    log("ordenado colunas")
    df = order_df(url=architecture_link, df=df)

    log("salvando csv")
    df.to_csv(
        path_output + "/" + f"{table_name}.csv",
        sep=",",
        index=False,
        na_rep="",
        encoding="utf-8",
    )

    del df

    return path_output
