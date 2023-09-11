# -*- coding: utf-8 -*-
"""
Tasks for br_ons_avaliacao_operacao
"""
import os
import pandas as pd
from datetime import datetime

from prefect import task
from pipelines.utils.utils import (
    log,
    to_partitions,
)
from pipelines.datasets.br_ons_avaliacao_operacao.constants import constants
from pipelines.datasets.br_ons_avaliacao_operacao.utils import (
    create_paths,
    crawler_ons,
    download_data as dw,
    change_columns_name,
    remove_latin1_accents_from_df,
    order_df,
    process_date_column,
    process_datetime_column,
    remove_decimal,
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

    dw(
        path=constants.PATH.value,
        url_list=url_list,
        table_name=table_name,
    )
    log("data downloaded")


@task
def wrang_data(
    table_name: str,
) -> pd.DataFrame:
    path_input = f"/tmp/br_ons_avaliacao_operacao/{table_name}/input"
    path_output = f"/tmp/br_ons_avaliacao_operacao/{table_name}/output"

    for file in os.listdir(path_input):
        if table_name == "reservatorio":
            log(f"fazendo {file}")
            file = path_input + "/" + file

            df = pd.read_csv(
                file,
                sep=";",
                decimal=".",
            )

            log("fazendo file")

            architecture_link = constants.TABLE_NAME_ARCHITECHTURE_DICT.value[
                table_name
            ]

            # rename cols
            df = change_columns_name(url=architecture_link, df=df)

            df["data"] = pd.to_datetime(df["data"]).dt.date

            df = remove_decimal(df, "id_reservatorio_planejamento")

            df = remove_decimal(df, "id_posto_vazao")

            df = remove_latin1_accents_from_df(df)

            df = order_df(url=architecture_link, df=df)

            df = df.to_csv(
                path_output + "/" + f"{table_name}.csv",
                sep=",",
                index=False,
                na_rep="",
                encoding="utf-8",
            )

        if (
            table_name == "energia_natural_afluente"
            or table_name == "energia_armazenada_reservatorio"
        ):
            # data da dd/mm/yyyy para yyyy-mm-dd

            log(f"fazendo {file}")
            file = path_input + "/" + file

            df = pd.read_csv(
                file,
                sep=";",
                decimal=".",
            )

            log("fazendo file")
            architecture_link = constants.TABLE_NAME_ARCHITECHTURE_DICT.value[
                table_name
            ]

            # rename cols
            df = change_columns_name(url=architecture_link, df=df)

            df = process_date_column(
                df=df,
                date_column="data",
            )
            log("1. as 5 primeiras linhas são:")
            log(df.head(5))

            df = remove_latin1_accents_from_df(df)

            df = order_df(url=architecture_link, df=df)
            log("2. as 5 primeiras linhas são:")
            log(df.head(5))
            to_partitions(
                data=df, partition_columns=["ano", "mes"], savepath=path_output
            )

            del df

        if (
            table_name == "geracao_usina"
            or table_name == "geracao_termica_motivo_despacho"
        ):
            print(f"fazendo {file}")
            file = path_input + "/" + file

            df = pd.read_csv(
                file,
                sep=";",
                thousands=".",
            )

            log("fazendo file")
            # rename cols
            architecture_link = constants.TABLE_NAME_ARCHITECHTURE_DICT.value[
                table_name
            ]

            df = change_columns_name(url=architecture_link, df=df)

            df = process_datetime_column(
                df=df,
                datetime_column="data",
            )
            log("datas formatadas")

            df = process_date_column(
                df=df,
                date_column="data",
            )

            log("datas formatadas")

            df = remove_latin1_accents_from_df(df)

            df = order_df(url=architecture_link, df=df)

            to_partitions(
                data=df, partition_columns=["ano", "mes"], savepath=path_output
            )

            del df

    return f"/tmp/br_ons_avaliacao_operacao/{table_name}/output"
