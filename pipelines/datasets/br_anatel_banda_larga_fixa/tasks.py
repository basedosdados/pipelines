# -*- coding: utf-8 -*-
"""
Tasks for br_anatel_banda_larga_fixa
"""
import pandas as pd
import numpy as np
from zipfile import ZipFile
from pathlib import Path


from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from prefect import task

from pipelines.utils.utils import (
    to_partitions,
    log,
)
from pipelines.datasets.br_anatel_banda_larga_fixa.utils import (
    check_and_create_column,
    download_file,
)
from pipelines.constants import constants


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def treatment():
    url = "https://www.anatel.gov.br/dadosabertos/paineis_de_dados/acessos/acessos_banda_larga_fixa.zip"

    download_dir = "/tmp/data/input"

    download_file(url=url, download_dir=download_dir)

    """anos = [
    "2007-2010",
    "2011-2012",
    "2013-2014",
    "2015-2016",
    "2017-2018",
    "2019-2020",
    "2021",
    "2022",
    "2023"]"""
    anos = ["2007-2010"]

    filepath = "/tmp/data/input/acessos_banda_larga_fixa.zip"

    # ! Abrindo o arquivo zipado
    with ZipFile(filepath) as z:

        # ! Iterando sobre a lista de anos
        for ano in anos:

            # ! Abrindo o arquivo csv dentro do zip pelo ano
            with z.open(f"Acessos_Banda_Larga_Fixa_{ano}.csv") as f:

                # ! Lendo o arquivo csv
                df = pd.read_csv(f, sep=";", encoding="utf-8")

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

                savepath = "/tmp/data/microdados.csv"
                # ! Fazendo referencia a função criada anteriormente para particionar o arquivo o arquivo
                to_partitions(
                    df,
                    partition_columns=["ano", "mes", "sigla_uf"],
                    savepath=savepath,
                )

    # ! retornando o caminho do path
    return savepath


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def treatment_br():
    url = "https://www.anatel.gov.br/dadosabertos/paineis_de_dados/acessos/acessos_banda_larga_fixa.zip"

    download_dir = "/tmp/data/input"

    download_file(url=url, download_dir=download_dir)

    filepath = "/tmp/data/input/acessos_banda_larga_fixa.zip"
    with ZipFile(filepath) as z:
        with z.open("Densidade_Banda_Larga_Fixa") as f:
            df = pd.read_csv(f, sep=";", encoding="utf-8")

            df.rename(columns={"Nível Geográfico Densidade": "Geografia"}, inplace=True)
            df_brasil = df[df["Geografia"] == "Brasil"]
            df_brasil = df_brasil.drop(
                ["UF", "Município", "Geografia", "Código IBGE"], axis=1
            )
            df_brasil["Densidade"] = df_brasil["Densidade"].apply(
                lambda x: float(x.replace(",", "."))
            )
            df_brasil.rename(
                columns={"Ano": "ano", "Mês": "mes", "Densidade": "densidade"},
                inplace=True,
            )

    return filepath


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def treatment_uf():
    url = "https://www.anatel.gov.br/dadosabertos/paineis_de_dados/acessos/acessos_banda_larga_fixa.zip"

    download_dir = "/tmp/data/input"

    download_file(url=url, download_dir=download_dir)

    filepath = "/tmp/data/input/acessos_banda_larga_fixa.zip"
    with ZipFile(filepath) as z:
        with z.open("Densidade_Banda_Larga_Fixa") as f:
            df = pd.read_csv(f, sep=";", enconding="utf-8")
            df_uf = df[df["Geografia"] == "UF"]
            df_uf.drop(["Município", "Código IBGE", "Geografia"], axis=1, inplace=True)
            df_uf["Densidade"] = df_uf["Densidade"].apply(
                lambda x: float(x.replace(",", "."))
            )
            df_uf.rename(
                columns={
                    "Ano": "ano",
                    "Mês": "mes",
                    "UF": "uf",
                    "Município": "municipio",
                    "Densidade": "densidade",
                },
                inplace=True,
            )

    return filepath


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def treatment_municipio():
    url = "https://www.anatel.gov.br/dadosabertos/paineis_de_dados/acessos/acessos_banda_larga_fixa.zip"

    download_dir = "/tmp/data/input"

    download_file(url=url, download_dir=download_dir)

    filepath = "/tmp/data/input/acessos_banda_larga_fixa.zip"
    with ZipFile(filepath) as z:
        with z.open("Densidade_Banda_Larga_Fixa") as f:
            df = pd.read_csv(f, sep=";", enconding="utf-8")
            df_municipio = df[df["Geografia"] == "Municipio"]
            df_municipio.drop(["Município", "UF", "Geografia"], axis=1, inplace=True)
            df_municipio["Densidade"] = df_municipio["Densidade"].apply(
                lambda x: float(x.replace(",", "."))
            )
            df_municipio.rename(
                columns={
                    "Ano": "ano",
                    "Mês": "mes",
                    "Código IBGE": "id_municipio",
                    "Densidade": "densidade",
                },
                inplace=True,
            )
            savepath = "/tmp/data/microdados.csv"

            # ! Fazendo referencia a função criada anteriormente para particionar o arquivo o arquivo
            to_partitions(
                df_municipio,
                partition_columns=["ano", "mes", "sigla_uf"],
                savepath=savepath,
            )

    return savepath
