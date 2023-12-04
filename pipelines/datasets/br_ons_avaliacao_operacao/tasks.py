# -*- coding: utf-8 -*-
"""
Tasks for br_ons_avaliacao_operacao
"""
import os
import time as tm
from datetime import date, datetime
from typing import Union

import pandas as pd
from prefect import task

from pipelines.datasets.br_ons_avaliacao_operacao.constants import constants
from pipelines.datasets.br_ons_avaliacao_operacao.utils import (
    change_columns_name,
    crawler_ons,
    create_paths,
)
from pipelines.datasets.br_ons_avaliacao_operacao.utils import download_data as dw
from pipelines.datasets.br_ons_avaliacao_operacao.utils import (
    extrai_data_recente,
    order_df,
    parse_year_or_year_month,
    process_date_column,
    process_datetime_column,
    remove_decimal,
    remove_latin1_accents_from_df,
)
from pipelines.utils.utils import log, to_partitions


@task
def download_data(
    table_name: str,
) -> None:
    """Essa task identifica a URL da tabela mais recente de uma das tabelas do ONS e baixa a tabela no formato csv

    Args:
        table_name (str): Nome da tabela, correponde ao table_id


    Returns:

    """
    # Cria paths
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

    if table_name == "reservatorio":
        dw(
            path=constants.PATH.value,
            url=url_list[0],
            table_name=table_name,
        )
    else:
        # usa dictionary comprehension para extrair data de cada link como key e link como item
        dicionario_data_url = {parse_year_or_year_month(url): url for url in url_list}

        tupla_data_maxima_url = max(dicionario_data_url.items(), key=lambda x: x[0])

        data_maxima = tupla_data_maxima_url[0]
        link_data_maxima = tupla_data_maxima_url[1]

        log(f"A data máxima é: {data_maxima}")
        log(f"A tabela será baixada de {link_data_maxima}")

        dw(
            path=constants.PATH.value,
            url=link_data_maxima,
            table_name=table_name,
        )

        log("O arquivo foi baixado!")
        tm.sleep(2)


# disclaimer -> -- 19/10/2023 --

# Objetivo: garantir que as tabelas estejam o mais atualiazadas possíveis
# sem a necessidade de fazer verificações manuais.
# Como fazer: algoritmo que verifica a data máxima das tabelas no Big Query e compara com a data
# máxima dos dados no portal de dados abertos do Operador Nacional do Sistema Elétrico (ONS).
# Problemas:
# 1. O ONS não apresenta um cronograma regular de atualização das bases.
# 2. Os arquivos divulgados não correspondem com a granularidade do dados. Tabelas com
# energia_armazenada_reservatorio são divulgadas com a granularidade de Datetime
# YYYY-MM-DD HH:MM  em arquivos mensais. Os dados são atualizados diariamente ou em intervalos
# de 2 ou 3 dias.
# 3. Não existe um metadado que informe a data mais recente dos dados nos arquivos divulgados.
# Solução:
# Baixar o arquivo mais recente e comparar com a data máxima das tabelas no Big Query.
# Se a data máxima do arquivo for maior que a data máxima das tabelas no Big Query,
# o flow sera atualizado.


@task
def wrang_data(
    table_name: str,
    data_mais_recente_do_bq: str,
) -> tuple[bool, str, Union[date, datetime]]:
    """Essa task realiza o tratamento dos dados das tabelas do ONS

    Args:
        table_name (str): nome da tabela, equivale ao table_id
        data_mais_recente_do_bq (Union[date,datetime]): Data mais recente da tabela no BQ extraida da api

    Returns:
        tuple[bool,str,Union[date,datetime]]: Retorna uma Tupla com 3 elementos:
        1. um valor booleano -> usado para parar o flow caso não haja atualização.
        2. uma string -> usada para informar o caminho dos dados tratados.
        3. um datetime ou date -> usada para atualizar o coverage
    """

    path_input = f"/tmp/br_ons_avaliacao_operacao/{table_name}/input"
    path_output = f"/tmp/br_ons_avaliacao_operacao/{table_name}/output"

    for file in os.listdir(path_input):
        file = path_input + "/" + file

        if table_name == "reservatorio":
            log(f"Construindo tabela {table_name}")

            df = pd.read_csv(
                file,
                sep=";",
                decimal=".",
            )

            #
            architecture_link = constants.TABLE_NAME_ARCHITECHTURE_DICT.value[
                table_name
            ]

            log("Renomeando colunas")
            df = change_columns_name(url=architecture_link, df=df)

            log("formatando data")
            df["data"] = pd.to_datetime(df["data"]).dt.date

            log("extraindo a data mais recente da tabela")
            data_tabela = extrai_data_recente(df=df, table_name=table_name)
            log(f"A data mais da tabela baixada é: ---- {data_tabela}")
            log(f"{type(data_tabela)}")
            log(f"A data mais recente do BQ é: ---- {data_mais_recente_do_bq}")
            log(f"{type(data_mais_recente_do_bq)}")

            if data_tabela > data_mais_recente_do_bq:
                log(
                    "A data mais recente do arquivo é maior que a data mais recente do BQ"
                )
                log("O flow de atualização será inciado")
            else:
                log("A data mais recente do arquivo é igual a data mais recente do BQ")
                log("O flow será terminado")
                return False, False

            log(
                "removendo decimais das colunas id_reservatorio_planejamento e id_posto_vazao"
            )
            df = remove_decimal(df, "id_reservatorio_planejamento")
            df = remove_decimal(df, "id_posto_vazao")

            log("removendo acentos")
            df = remove_latin1_accents_from_df(df)

            log("odernando colunas")
            df = order_df(url=architecture_link, df=df)

            log("salvando tabela")
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

            log(f"Construindo tabela {table_name}")

            df = pd.read_csv(
                file,
                sep=";",
                decimal=".",
            )

            architecture_link = constants.TABLE_NAME_ARCHITECHTURE_DICT.value[
                table_name
            ]

            log("Renomeando colunas")
            df = change_columns_name(url=architecture_link, df=df)

            log("criando colunas de ano e mes")
            df = process_date_column(
                df=df,
                date_column="data",
            )

            log("extraindo a data mais recente do BQ")
            data_tabela = extrai_data_recente(df=df, table_name=table_name)
            log(f"A data mais da tabela baixada é: ---- {data_tabela}")
            log(f"A data mais recente do BQ é: ---- {data_mais_recente_do_bq}")

            if data_tabela > data_mais_recente_do_bq:
                log(
                    "A data mais recente do arquivo é maior que a data mais recente do BQ"
                )
                log("O flow de atualização será inciado")
            else:
                log("A data mais recente do arquivo é igual a data mais recente do BQ")
                log("O flow será terminado")
                return False, False

            log("removendo acentos")
            df = remove_latin1_accents_from_df(df)

            log("odernando colunas")
            df = order_df(url=architecture_link, df=df)

            log("particionando dados")
            to_partitions(
                data=df, partition_columns=["ano", "mes"], savepath=path_output
            )

            del df

        if (
            table_name == "geracao_usina"
            or table_name == "geracao_termica_motivo_despacho"
        ):
            log(f"Construindo tabela {table_name}")

            df = pd.read_csv(
                file,
                sep=";",
                thousands=".",
            )

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

            log("extraindo a data mais recente da tabela")
            data_tabela = extrai_data_recente(df=df, table_name=table_name)
            log(f"A data mais da tabela baixada é: ---- {data_tabela}")
            log(f"{type(data_tabela)}")
            log(f"A data mais recente do BQ é: ---- {data_mais_recente_do_bq}")
            log(f"{type(data_mais_recente_do_bq)}")

            if data_tabela > data_mais_recente_do_bq:
                log(
                    "A data mais recente do arquivo é maior que a data mais recente do BQ"
                )
                log("O flow de atualização será inciado")
            else:
                log("A data mais recente do arquivo é igual a data mais recente do BQ")
                log("O flow será terminado")
                return False, False

            log("removendo acentos")
            df = remove_latin1_accents_from_df(df)

            log("ordenando colunas")
            df = order_df(url=architecture_link, df=df)

            to_partitions(
                data=df, partition_columns=["ano", "mes"], savepath=path_output
            )

            del df

        if table_name == "restricao_operacao_usinas_eolicas":
            log(f"Construindo tabela {table_name}")

            df = pd.read_csv(
                file,
                sep=";",
                decimal=".",
                # todo: verificar se funciona
            )

            architecture_link = constants.TABLE_NAME_ARCHITECHTURE_DICT.value[
                table_name
            ]
            # rename cols
            df = change_columns_name(url=architecture_link, df=df)

            df = process_datetime_column(
                df=df,
                datetime_column="data",
            )

            log("criando colunas de ano e mes")
            df = process_date_column(
                df=df,
                date_column="data",
            )

            log("extraindo a data mais recente da tabela")
            data_tabela = extrai_data_recente(df=df, table_name=table_name)
            log(f"A data mais da tabela baixada é: ---- {data_tabela}")
            log(f"{type(data_tabela)}")
            log(f"A data mais recente do BQ é: ---- {data_mais_recente_do_bq}")
            log(f"{type(data_mais_recente_do_bq)}")

            if data_tabela > data_mais_recente_do_bq:
                log(
                    "A data mais recente do arquivo é maior que a data mais recente do BQ"
                )
                log("O flow de atualização será inciado")
            else:
                log("A data mais recente do arquivo é igual a data mais recente do BQ")
                log("O flow será terminado")
                return False, False

            log("datas formatadas")

            df = remove_latin1_accents_from_df(df)

            df = order_df(url=architecture_link, df=df)

            to_partitions(
                data=df, partition_columns=["ano", "mes"], savepath=path_output
            )

            del df

    return True, path_output, data_tabela
