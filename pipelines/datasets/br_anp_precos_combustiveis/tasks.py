# -*- coding: utf-8 -*-
"""
Tasks for br_anp_precos_combustiveis
"""

from prefect import task
import pandas as pd
import numpy as np
import basedosdados as bd
import unidecode
from datetime import timedelta
from pipelines.datasets.br_anp_precos_combustiveis.utils import (
    download_files,
    get_id_municipio,
)
from pipelines.datasets.br_anp_precos_combustiveis.constants import (
    constants as anatel_constants,
)
from pipelines.utils.utils import to_partitions, log
from pipelines.constants import constants


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def tratamento():
    download_files(
        [
            "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/qus/ultimas-4-semanas-glp.csv",
            "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/qus/ultimas-4-semanas-gasolina-etanol.csv",
            "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/qus/ultimas-4-semanas-diesel-gnv.csv",
        ],
        "/tmp/input/input/",
    )

    log("----" * 150)
    data_frames = []
    log("Abrindo os arquivos csvs")
    diesel = pd.read_csv(
        f"/tmp/input/ultimas-4-semanas-diesel-gnv.csv", sep=";", encoding="utf-8"
    )
    log("Abrindo os arquivos csvs", diesel)
    gasolina = pd.read_csv(
        f"/tmp/input/ultimas-4-semanas-gasolina-etanol.csv", sep=";", encoding="utf-8"
    )
    log("Abrindo os arquivos csvs", gasolina)
    glp = pd.read_csv(
        f"/tmp/input/ultimas-4-semanas-glp.csv", sep=";", encoding="utf-8"
    )
    log("Abrindo os arquivos csvs", glp)
    data_frames.extend([diesel, gasolina, glp])
    precos_combustiveis = pd.concat(data_frames, ignore_index=True)
    log("----" * 150)
    log("Dados concatenados com sucesso")
    log("----" * 150)
    get_id_municipio()
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
    precos_combustiveis.rename(columns=anatel_constants.rename.value, inplace=True)
    precos_combustiveis = precos_combustiveis[anatel_constants.ordem.value]
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
    to_partitions(
        precos_combustiveis,
        partition_columns=["ano", "data_coleta"],
        savepath="/tmp/output/",
    )
