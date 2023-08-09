# -*- coding: utf-8 -*-
"""
Tasks for br_anp_precos_combustiveis
"""

from prefect import task
import pandas as pd
import numpy as np
from datetime import timedelta
from pipelines.datasets.br_anp_precos_combustiveis.utils import (
    download_files,
    get_id_municipio,
    open_csvs,
    partition_data,
)
from pipelines.datasets.br_anp_precos_combustiveis.constants import (
    constants as anatel_constants,
)
from pipelines.utils.utils import log
from pipelines.constants import constants


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def tratamento():
    download_files(
        anatel_constants.URLS.value,
        anatel_constants.PATH_INPUT.value,
    )

    precos_combustiveis = open_csvs(
        url_diesel_gnv=anatel_constants.URL_DIESEL_GNV.value,
        url_gasolina_etanol=anatel_constants.URL_GASOLINA_ETANOL.value,
        url_glp=anatel_constants.URL_GLP.value,
    )

    id_municipio = get_id_municipio()
    log("Iniciando tratamento dos dados precos_combustiveis")
    precos_combustiveis = pd.merge(
        id_municipio,
        precos_combustiveis,
        how="right",
        left_on=["nome", "sigla_uf"],
        right_on=["Municipio", "Estado - Sigla"],
    )
    log("----" * 150)
    log("Dados mergeados")
    precos_combustiveis.rename(columns={"Municipio": "nome"}, inplace=True)
    precos_combustiveis.dropna(subset=["Valor de Venda"], inplace=True)
    precos_combustiveis["endereco_revenda"] = (
        precos_combustiveis["Nome da Rua"].fillna("")
        + ","
        + " "
        + precos_combustiveis["Numero Rua"].fillna("")
        + ","
        + " "
        + precos_combustiveis["Complemento"].fillna("")
    )
    precos_combustiveis.drop(columns=["sigla_uf"], inplace=True)
    precos_combustiveis.rename(columns={"Data da Coleta": "data_coleta"}, inplace=True)
    precos_combustiveis["data_coleta"] = (
        precos_combustiveis["data_coleta"].str[6:10]
        + "-"
        + precos_combustiveis["data_coleta"].str[3:5]
        + "-"
        + precos_combustiveis["data_coleta"].str[0:2]
    )
    precos_combustiveis["Produto"] = precos_combustiveis["Produto"].str.lower()
    precos_combustiveis["ano"] = precos_combustiveis["data_coleta"].str[0:4]
    precos_combustiveis["ano"].replace("nan", "", inplace=True)
    precos_combustiveis.rename(columns=anatel_constants.RENAME.value, inplace=True)
    precos_combustiveis = precos_combustiveis[anatel_constants.ORDEM.value]
    precos_combustiveis["ano"] = precos_combustiveis["ano"].apply(
        lambda x: str(x).replace(".0", "")
    )
    precos_combustiveis["cep_revenda"] = precos_combustiveis["cep_revenda"].apply(
        lambda x: str(x).replace("-", "")
    )
    precos_combustiveis["unidade_medida"] = precos_combustiveis["unidade_medida"].map(
        {"R$ / litro": "R$/litro", "R$ / m³": "R$/m3", "R$ / 13 kg": "R$/13kg"}
    )
    precos_combustiveis["nome_estabelecimento"] = precos_combustiveis[
        "nome_estabelecimento"
    ].apply(lambda x: str(x).replace(",", ""))
    precos_combustiveis["preco_compra"] = precos_combustiveis["preco_compra"].apply(
        lambda x: str(x).replace(",", ".")
    )
    precos_combustiveis["preco_venda"] = precos_combustiveis["preco_venda"].apply(
        lambda x: str(x).replace(",", ".")
    )
    precos_combustiveis["preco_venda"] = precos_combustiveis["preco_venda"].replace(
        "nan", ""
    )
    precos_combustiveis["preco_compra"] = precos_combustiveis["preco_compra"].replace(
        "nan", ""
    )
    precos_combustiveis.replace(np.nan, "", inplace=True)
    log("----" * 150)
    log("Dados tratados com sucesso")
    log("----" * 150)
    log("Iniciando particionamento dos dados")
    log("----" * 150)
    log(precos_combustiveis["data_coleta"].unique())
    partition_data(
        precos_combustiveis,
        column_name="data_coleta",
        output_directory=anatel_constants.PATH_OUTPUT.value,
    )

    return anatel_constants.PATH_OUTPUT.value


def data_max_bd_mais():
    log("----" * 150)
    log("update_metadata bd pro")
    data_frames = []
    diesel = pd.read_csv(
        f"{anatel_constants.URL_DIESEL_GNV.value}", sep=";", encoding="utf-8"
    )
    gasolina = pd.read_csv(
        f"{anatel_constants.URL_GASOLINA_ETANOL.value}", sep=";", encoding="utf-8"
    )
    glp = pd.read_csv(f"{anatel_constants.URL_GLP.value}", sep=";", encoding="utf-8")
    data_frames.extend([diesel, gasolina, glp])
    precos_combustiveis = pd.concat(data_frames, ignore_index=True)
    precos_combustiveis["Data da Coleta"] = (
        precos_combustiveis["Data da Coleta"].str[6:10]
        + "-"
        + precos_combustiveis["Data da Coleta"].str[3:5]
        + "-"
        + precos_combustiveis["Data da Coleta"].str[0:2]
    )
    data_max = precos_combustiveis["Data da Coleta"].max()

    return data_max


def data_max_bd_pro():
    log("----" * 150)
    log("update_metadata bd pro")
    data_frames = []
    diesel = pd.read_csv(
        f"{anatel_constants.URL_DIESEL_GNV.value}", sep=";", encoding="utf-8"
    )
    gasolina = pd.read_csv(
        f"{anatel_constants.URL_GASOLINA_ETANOL.value}", sep=";", encoding="utf-8"
    )
    glp = pd.read_csv(f"{anatel_constants.URL_GLP.value}", sep=";", encoding="utf-8")
    data_frames.extend([diesel, gasolina, glp])
    precos_combustiveis = pd.concat(data_frames, ignore_index=True)
    precos_combustiveis["Data da Coleta"] = (
        precos_combustiveis["Data da Coleta"].str[6:10]
        + "-"
        + precos_combustiveis["Data da Coleta"].str[3:5]
        + "-"
        + precos_combustiveis["Data da Coleta"].str[0:2]
    )
    precos_combustiveis["Data da Coleta"] = pd.to_datetime(
        precos_combustiveis["Data da Coleta"], format="%Y-%m-%d"
    )

    data_max = precos_combustiveis["Data da Coleta"].max()
    data_referencia = data_max - pd.DateOffset(months=6)
    data_referencia = data_referencia.strftime("%Y-%m-%d")

    return data_referencia
