# -*- coding: utf-8 -*-
"""
Tasks for br_anp_precos_combustiveis
"""

# ! Importando as bibliotecas

import pandas as pd
import basedosdados as bd
import unidecode

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from prefect import task

from pipelines.utils.utils import (
    to_partitions,
)
from pipelines.datasets.br_anp_precos_combustiveis.utils import (
    download_files,
)
from pipelines.constants import constants


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def tratamento():

    rename = {
        "Revenda": "nome_estabelecimento",
        "CNPJ da Revenda": "cnpj_revenda",
        "Bairro": "bairro_revenda",
        "Cep": "cep_revenda",
        "Produto": "produto",
        "Valor de Venda": "preco_venda",
        "Valor de Compra": "preco_compra",
        "Unidade de Medida": "unidade_medida",
        "Bandeira": "bandeira_revenda",
        "Estado - Sigla": "sigla_uf",
        "Municipio": "nome",
        "Data da Coleta": "data_coleta",
        "Nome da Rua": "nome_rua",
        "Numero Rua": "numero_rua",
        "Complemento": "complemento",
    }

    ordem = [
        "ano",
        "sigla_uf",
        "id_municipio",
        "bairro_revenda",
        "cep_revenda",
        "endereco_revenda",
        "cnpj_revenda",
        "nome_estabelecimento",
        "bandeira_revenda",
        "data_coleta",
        "produto",
        "unidade_medida",
        "preco_compra",
        "preco_venda",
    ]

    # ! Carregando os dados direto do Diretório de municipio da BD
    # Para carregar o dado direto no pandas
    id_municipio = bd.read_table(
        dataset_id="br_bd_diretorios_brasil",
        table_id="municipio",
        billing_project_id="basedosdados-dev",
    )

    # ! Tratamento do id_municipio para mergear com a base
    id_municipio["nome"] = id_municipio["nome"].str.upper()
    id_municipio["nome"] = id_municipio["nome"].apply(unidecode.unidecode)
    id_municipio["nome"] = id_municipio["nome"].replace(
        "ESPIGAO D'OESTE", "ESPIGAO DO OESTE"
    )
    id_municipio["nome"] = id_municipio["nome"].replace(
        "SANT'ANA DO LIVRAMENTO", "SANTANA DO LIVRAMENTO"
    )
    id_municipio = id_municipio[["id_municipio", "nome"]]

    download_files()

    for a in {*range(2004, 2023)}:

        df = pd.read_csv(
            f"input/ca-{a}-01.csv", sep=";", encoding="ISO-8859-1"
        )  # ! combustiveis automativos 2004 - Primeiro semestre

        dh = pd.read_csv(
            f"input/glp-{a}-01.csv", sep=";", encoding="ISO-8859-1"
        )  # ! glp 2004 - Primeiro semestre

        dk = pd.read_csv(
            f"input/ca-{a}-02.csv", sep=";", encoding="ISO-8859-1"
        )  # ! combustiveis automativos 2004 - Primeiro semestre

        dz = pd.read_csv(
            f"input/glp-{a}-02.csv", sep=";", encoding="ISO-8859-1"
        )  # ! glp 2004 - Primeiro semestre

        df_fx = df.append([dh, dk, dz], ignore_index=True)

        df_fx = pd.merge(
            id_municipio, df_fx, how="right", left_on=["nome"], right_on=["Municipio"]
        )

        df_fx.rename(columns={"Municipio": "nome"}, inplace=True)

        df_fx["endereco_revenda"] = (
            df_fx["Nome da Rua"].fillna("")
            + ","
            + " "
            + df_fx["Numero Rua"].fillna("")
            + ","
            + " "
            + df_fx["Complemento"].fillna("")
        )

        df_fx.rename(columns={"Data da Coleta": "data_coleta"}, inplace=True)

        df_fx = df_fx.query('data_coleta != "08/"')

        df_fx["data_coleta"] = pd.to_datetime(df_fx["data_coleta"], format="%d/%m/%Y")

        df_fx["Produto"] = df_fx["Produto"].str.lower()

        df_fx["ano"] = df_fx["data_coleta"].dt.year

        df_fx["ano"].replace("nan", "", inplace=True)

        df_fx.rename(columns=rename, inplace=True)

        df_fx = df_fx[ordem]

        df_fx["ano"] = df_fx["ano"].apply(lambda x: str(x).replace(".0", ""))

        df_fx["cep_revenda"] = df_fx["cep_revenda"].apply(
            lambda x: str(x).replace("-", "")
        )

        df_fx["unidade_medida"] = df_fx["unidade_medida"].map(
            {"R$ / litro": "R$/litro", "R$ / m³": "R$/m3", "R$ / 13 kg": "R$/13kg"}
        )

        df_fx["nome_estabelecimento"] = df_fx["nome_estabelecimento"].apply(
            lambda x: str(x).replace(",", "")
        )

        df_fx["preco_compra"] = df_fx["preco_compra"].apply(
            lambda x: str(x).replace(",", ".")
        )

        df_fx["preco_venda"] = df_fx["preco_venda"].apply(
            lambda x: str(x).replace(",", ".")
        )

        df_fx["preco_venda"] = df_fx["preco_venda"].replace("nan", "")

        df_fx["preco_compra"] = df_fx["preco_compra"].replace("nan", "")

        df_fx.sort_values("data_coleta", inplace=True)

        df_fx.drop_duplicates()

        to_partitions(df_fx, partition_columns=["ano"], savepath="/tmp/output/")
