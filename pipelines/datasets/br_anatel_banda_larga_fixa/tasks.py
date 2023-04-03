# -*- coding: utf-8 -*-
"""
Tasks for br_anatel_banda_larga_fixa
"""
import pandas as pd
import numpy as np
import os
import zipfile
from zipfile import ZipFile
from pathlib import Path

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from prefect import task
import requests
import pandas as pd

from pipelines.utils.utils import (
    log,
)
from pipelines.datasets.br_bd_metadados.utils import (
    get_temporal_coverage_list,
)
from pipelines.constants import constants

from prefect import task


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def to_partitions(df: pd.DataFrame, partition_columns: list[str], savepath: str):
    """# ! Salve os dados no esquema de partições do hive, dado um dataframe e uma lista de colunas de partição.

    # * Argumentos:

        # ! data (pandas.core.frame.DataFrame): Dataframe a ser particionado.
        # ! partition_columns (list): Lista de colunas a serem usadas como partições.
        # ! savepath (str, pathlib.PosixPath): caminho da pasta para salvar as partições.

    # * Exemple:

        # ! data = {
        # !    "ano": [2020, 2021, 2020, 2021, 2020, 2021, 2021,2025],
        # !    "mes": [1, 2, 3, 4, 5, 6, 6,9],
        # !    "sigla_uf": ["SP", "SP", "RJ", "RJ", "PR", "PR", "PR","PR"],
        # !    "dado": ["a", "b", "c", "d", "e", "f", "g",'h']}

        # ! to_partitions(
        # !    data=pd.DataFrame(data),
        # !    partition_columns=['ano','mes','sigla_uf'],
        # !    savepath='partitions/')
    """

    if isinstance(df, (pd.core.frame.DataFrame)):

        savepath = Path(savepath)

        # ! criar combinações únicas entre colunas de partição
        unique_combinations = (
            df[partition_columns]
            .drop_duplicates(subset=partition_columns)
            .to_dict(orient="records")
        )

        for filter_combination in unique_combinations:
            patitions_values = [
                f"{partition}={value}"
                for partition, value in filter_combination.items()
            ]

            # get filtered data
            df_filter = df.loc[
                df[filter_combination.keys()]
                .isin(filter_combination.values())
                .all(axis=1),
                :,
            ]
            df_filter = df_filter.drop(columns=partition_columns)

            # ! criar árvore de pastas
            filter_save_path = Path(savepath / "/".join(patitions_values))
            filter_save_path.mkdir(parents=True, exist_ok=True)
            file_filter_save_path = Path(filter_save_path) / "microdados.csv"

            # ! anexar dados ao csv
            df_filter.to_csv(
                file_filter_save_path,
                index=False,
                mode="a",
                header=not file_filter_save_path.exists(),
                encoding="utf-8",
            )

    else:
        raise BaseException("Os dados precisam ser um DataFrame do pandas")


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def check_and_create_column(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """
    # ! Verifique se existe uma coluna em um Pandas DataFrame. Caso contrário, crie uma nova coluna com o nome fornecido
    # ! e preenchê-lo com valores NaN. Se existir, não faça nada.

    # * Parâmetros:
    # ! df (Pandas DataFrame): O DataFrame a ser verificado.
    # ! col_name (str): O nome da coluna a ser verificada ou criada.

    # * Retorna:
    # ! Pandas DataFrame: O DataFrame modificado.
    """

    if col_name not in df.columns:
        df[col_name] = ""
    return df


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def treatment():

    # ! Crio os diretórios de entrada
    os.makedirs("/tmp/data/input", exist_ok=True)

    # ! url do arquivo zip e baixo os arquivos
    url = "https://www.anatel.gov.br/dadosabertos/paineis_de_dados/acessos/acessos_banda_larga_fixa.zip"
    response = requests.get(url)

    # ! Abrindo o arquivo zip
    with open("/tmp/data/input/acessos_banda_larga_fixa.zip", "wb") as zip_file:
        zip_file.write(response.content)

    # ! Descompactando o arquivo zip
    with zipfile.ZipFile(
        "/tmp/data/input/acessos_banda_larga_fixa.zip", "r"
    ) as zip_ref:
        zip_ref.extractall("/tmp/data/input")

    # ! Criando o caminho para o arquivo zipado
    pasta = "/tmp/data/input"
    banda_larga = os.path.join(pasta, "acessos_banda_larga_fixa.zip")

    """anos = [
        "2007-2010",
        "2011-2012",
        "2013-2014",
        "2015-2016",
        "2017-2018",
        "2019-2020",
        "2021",
        "2022",
        "2023",
    ]"""  # ! Lista de anos a serem processados
    anos = ["2007-2010"]
    # ! Abrindo o arquivo zipado
    with ZipFile(banda_larga) as z:

        # ! Iterando sobre a lista de anos
        for ano in anos:

            # ! Abrindo o arquivo csv dentro do zip pelo ano
            with z.open(f"Acessos_Banda_Larga_Fixa_{ano}.csv") as f:

                # ! Lendo o arquivo csv
                df = pd.read_csv(f, sep=";", encoding="utf-8")
                # ! Fazendo referencia a função criada anteriormente para verificar colunas
                df = check_and_create_column.run(df, "Tipo de Produto")

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

                # ! Removendo colunas "grupo_economico" e "municipio"
                # organização das variáveis
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

                # ! Fazendo referencia a função criada anteriormente para particionar o arquivo o arquivo
                to_partitions.run(
                    df=df,
                    partition_columns=["ano", "mes", "sigla_uf"],
                    savepath="/tmp/data/microdados.csv",
                )

    # ! retornando o caminho do path
    return "/tmp/data/microdados.csv"
