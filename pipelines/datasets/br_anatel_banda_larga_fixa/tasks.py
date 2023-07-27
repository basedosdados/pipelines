# -*- coding: utf-8 -*-
"""
Tasks for dataset br_anatel_banda_larga_fixa
"""
import pandas as pd
import numpy as np
from zipfile import ZipFile
from pathlib import Path
import os

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from prefect import task
from pipelines.datasets.br_anatel_banda_larga_fixa.constants import (
    constants as anatel_constants,
)
from pipelines.utils.utils import (
    to_partitions,
    log,
)
from pipelines.datasets.br_anatel_banda_larga_fixa.utils import (
    check_and_create_column,
    download_and_unzip,
)
from pipelines.constants import constants


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def treatment(ano: int):
    log("Iniciando o tratamento do arquivo microdados da Anatel")
    download_and_unzip(
        url=anatel_constants.URL.value, path=anatel_constants.INPUT_PATH.value
    )

    # ! Lendo o arquivo csv
    df = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Acessos_Banda_Larga_Fixa_{ano}.csv",
        sep=";",
        encoding="utf-8",
    )

    # ! Fazendo referencia a função criada anteriormente para verificar colunas
    df = check_and_create_column(df, "Tipo de Produto")

    # ! Renomeando as colunas
    df.rename(
        columns={
            "Ano": "ano",
            "Mês": "mes",
            "Grupo Econômico": "grupo_economico",
            "Empresa": "empresa",
            "CNPJ": "cnpj",
            "Porte da Prestadora": "porte_empresa",
            "UF": "sigla_uf",
            "Município": "municipio",
            "Código IBGE Município": "id_municipio",
            "Faixa de Velocidade": "velocidade",
            "Tecnologia": "tecnologia",
            "Meio de Acesso": "transmissao",
            "Acessos": "acessos",
            "Tipo de Pessoa": "pessoa",
            "Tipo de Produto": "produto",
        },
        inplace=True,
    )

    # ! organização das variáveis
    df.drop(["grupo_economico", "municipio"], axis=1, inplace=True)

    # ! Reordenando as colunas
    df = df[
        [
            "ano",
            "mes",
            "sigla_uf",
            "id_municipio",
            "cnpj",
            "empresa",
            "porte_empresa",
            "tecnologia",
            "transmissao",
            "velocidade",
            "produto",
            "acessos",
        ]
    ]

    # ! Classificação do DataFrame em ordem crescente
    df.sort_values(
        [
            "ano",
            "mes",
            "sigla_uf",
            "id_municipio",
            "cnpj",
            "empresa",
            "porte_empresa",
            "tecnologia",
            "transmissao",
            "velocidade",
        ],
        inplace=True,
    )
    # ! substituindo valores nulos por vazio
    df.replace(np.nan, "", inplace=True)
    # ! Retirando os acentos da coluna "transmissao"
    df["transmissao"] = df["transmissao"].apply(
        lambda x: x.replace("Cabo Metálico", "Cabo Metalico")
        .replace("Satélite", "Satelite")
        .replace("Híbrido", "Hibrido")
        .replace("Fibra Óptica", "Fibra Optica")
        .replace("Rádio", "Radio")
    )

    df["acessos"] = df["acessos"].apply(lambda x: str(x).replace(".0", ""))

    df["produto"] = df["produto"].apply(
        lambda x: x.replace("LINHA_DEDICADA", "linha dedicada").lower()
    )
    log("Salvando o arquivo microdados da Anatel")
    # ! Fazendo referencia a função criada anteriormente para particionar o arquivo o arquivo
    to_partitions(
        df,
        partition_columns=["ano", "mes", "sigla_uf"],
        savepath=anatel_constants.OUTPUT_PATH_MICRODADOS.value,
    )

    # ! retornando o caminho do path
    return anatel_constants.OUTPUT_PATH_MICRODADOS.value


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def treatment_br():
    log("Iniciando o tratamento do arquivo densidade brasil da Anatel")
    df = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Densidade_Banda_Larga_Fixa.csv",
        sep=";",
        encoding="utf-8",
    )

    # ! Tratando o csv
    df.rename(columns={"Nível Geográfico Densidade": "Geografia"}, inplace=True)
    df_brasil = df[df["Geografia"] == "Brasil"]
    df_brasil = df_brasil.drop(["UF", "Município", "Geografia", "Código IBGE"], axis=1)
    df_brasil["Densidade"] = df_brasil["Densidade"].apply(
        lambda x: float(x.replace(",", "."))
    )
    df_brasil.rename(
        columns={"Ano": "ano", "Mês": "mes", "Densidade": "densidade"},
        inplace=True,
    )
    # Cria um diretório de saída, se não existir
    os.system(f"mkdir -p {anatel_constants.OUTPUT_PATH_BRASIL.value}")
    df_brasil.to_csv(
        f"{anatel_constants.OUTPUT_PATH_BRASIL.value}densidade_brasil.csv",
        index=False,
        sep=",",
        encoding="utf-8",
        na_rep="",
    )
    return anatel_constants.OUTPUT_PATH_BRASIL.value


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def treatment_uf():
    log("Iniciando o tratamento do arquivo densidade uf da Anatel")
    df = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Densidade_Banda_Larga_Fixa.csv",
        sep=";",
        encoding="utf-8",
    )
    df.rename(columns={"Nível Geográfico Densidade": "Geografia"}, inplace=True)
    df_uf = df[df["Geografia"] == "UF"]
    df_uf.drop(["Município", "Código IBGE", "Geografia"], axis=1, inplace=True)
    df_uf["Densidade"] = df_uf["Densidade"].apply(lambda x: float(x.replace(",", ".")))
    df_uf.rename(
        columns={
            "Ano": "ano",
            "Mês": "mes",
            "UF": "sigla_uf",
            "Densidade": "densidade",
        },
        inplace=True,
    )

    # ! Salvando o csv tratado
    os.system(f"mkdir -p {anatel_constants.OUTPUT_PATH_UF.value}")
    df_uf.to_csv(
        f"{anatel_constants.OUTPUT_PATH_UF.value}densidade_uf.csv",
        index=False,
        sep=",",
        encoding="utf-8",
        na_rep="",
    )

    return anatel_constants.OUTPUT_PATH_UF.value


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def treatment_municipio():
    log("Iniciando o tratamento do arquivo densidade municipio da Anatel")
    df = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Densidade_Banda_Larga_Fixa.csv",
        sep=";",
        encoding="utf-8",
    )

    # ! Tratando o csv
    df.rename(columns={"Nível Geográfico Densidade": "Geografia"}, inplace=True)
    df_municipio = df[df["Geografia"] == "Municipio"]
    df_municipio.drop(["Município", "Geografia"], axis=1, inplace=True)
    df_municipio["Densidade"] = df_municipio["Densidade"].apply(
        lambda x: float(x.replace(",", "."))
    )
    df_municipio.rename(
        columns={
            "Ano": "ano",
            "Mês": "mes",
            "UF": "sigla_uf",
            "Código IBGE": "id_municipio",
            "Densidade": "densidade",
        },
        inplace=True,
    )
    log("Salvando o arquivo densidade municipio da Anatel")
    # ! Fazendo referencia a função criada anteriormente para particionar o arquivo o arquivo
    to_partitions(
        df_municipio,
        partition_columns=["ano", "mes", "sigla_uf"],
        savepath=anatel_constants.OUTPUT_PATH_MUNICIPIO.value,
    )

    return anatel_constants.OUTPUT_PATH_MUNICIPIO.value


# task para retornar o ano e mes paara a atualização dos metadados.
@task
def get_today_date():
    d = datetime.now() - timedelta(days=60)
    return d.strftime("%Y-%m")
