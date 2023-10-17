# -*- coding: utf-8 -*-
"""
Tasks for br_ons_avaliacao_operacao
"""
import os
import time as tm
from datetime import datetime

import pandas as pd
from prefect import task

from pipelines.datasets.br_ons_estimativa_custos.constants import constants
from pipelines.datasets.br_ons_estimativa_custos.utils import (
    change_columns_name,
    crawler_ons,
    create_paths,
)
from pipelines.datasets.br_ons_estimativa_custos.utils import download_data as dw
from pipelines.datasets.br_ons_estimativa_custos.utils import (
    download_data_final,
    order_df,
    parse_year_or_year_month,
    process_date_column,
    process_datetime_column,
    remove_latin1_accents_from_df,
)
from pipelines.utils.utils import log


@task
def get_today_date():
    d = datetime.today()

    return d.strftime("%Y-%m-%d")


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
        table_name=table_name,
    )
    log("paths created")

    url_list = crawler_ons(
        url=constants.TABLE_NAME_URL_DICT.value[table_name],
    )
    log("As urls foram recuperadas")
    tm.sleep(2)
    # usa dictionary comprehension para extrair data de cada link como key e link como item
    dicionario_data_url = {parse_year_or_year_month(url): url for url in url_list}

    # so tirar essa etapa para subir dados históricos
    tupla_data_maxima_url = max(dicionario_data_url.items(), key=lambda x: x[0])

    data_maxima = tupla_data_maxima_url[0]
    link_data_maxima = tupla_data_maxima_url[1]

    log(f"A data máxima é: {data_maxima}")
    log(f"A tabela será baixada de {link_data_maxima}")

    download_data_final(
        path=constants.PATH.value,
        url=link_data_maxima,
        table_name=table_name,
    )
    log("O arquivo foi baixado!")
    tm.sleep(2)


@task
def wrang_data(
    table_name: str,
) -> pd.DataFrame:
    path_input = f"/tmp/br_ons_estimativa_custos/{table_name}/input"
    path_output = f"/tmp/br_ons_estimativa_custos/{table_name}/output"

    df_list = []

    # todo: tirar loop já que será somente um arquivo
    # todo: carregar dados historicos do CVU
    for file in os.listdir(path_input):
        if table_name == "custo_marginal_operacao_semanal":
            log(f"fazendo {file}")
            file1 = path_input + "/" + file
            log(f"{file1}")

            df = pd.read_csv(
                file1,
                sep=";",
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
            log("df appended")
            del df
            log("df deleted")

        if table_name == "balanco_energia_subsistemas":
            file = path_input + "/" + file

            df = pd.read_csv(
                file,
                sep=";",
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

        if table_name == "custo_variavel_unitario_usinas_termicas":
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

            log("datas formatadas")

        else:
            log(f"fazendo {file}")
            file = path_input + "/" + file

            df = pd.read_csv(
                file,
                sep=";",
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
    log("ordenando colunas")
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
