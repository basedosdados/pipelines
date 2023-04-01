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
    """Save data in to hive patitions schema, given a dataframe and a list of partition columns.
    Args:
        data (pandas.core.frame.DataFrame): Dataframe to be partitioned.
        partition_columns (list): List of columns to be used as partitions.
        savepath (str, pathlib.PosixPath): folder path to save the partitions
    Exemple:
        data = {
            "ano": [2020, 2021, 2020, 2021, 2020, 2021, 2021,2025],
            "mes": [1, 2, 3, 4, 5, 6, 6,9],
            "sigla_uf": ["SP", "SP", "RJ", "RJ", "PR", "PR", "PR","PR"],
            "dado": ["a", "b", "c", "d", "e", "f", "g",'h'],
        }
        to_partitions(
            data=pd.DataFrame(data),
            partition_columns=['ano','mes','sigla_uf'],
            savepath='partitions/'
        )
    """

    if isinstance(df, (pd.core.frame.DataFrame)):

        savepath = Path(savepath)

        # create unique combinations between partition columns
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

            # create folder tree
            filter_save_path = Path(savepath / "/".join(patitions_values))
            filter_save_path.mkdir(parents=True, exist_ok=True)
            file_filter_save_path = Path(filter_save_path) / "microdados.csv"

            # append data to csv
            df_filter.to_csv(
                file_filter_save_path,
                index=False,
                mode="a",
                header=not file_filter_save_path.exists(),
                encoding="utf-8",
            )

    else:
        raise BaseException("Data need to be a pandas DataFrame")


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def check_and_create_column(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """
    Verifique se existe uma coluna em um Pandas DataFrame. Caso contrário, crie uma nova coluna com o nome fornecido
    e preenchê-lo com valores NaN. Se existir, não faça nada.

    Parâmetros:
    df (Pandas DataFrame): O DataFrame a ser verificado.
    col_name (str): O nome da coluna a ser verificada ou criada.

    Retorna:
    Pandas DataFrame: O DataFrame modificado.
    """
    if col_name not in df.columns:
        df[col_name] = ""
    return df


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def treatment():
    os.makedirs("/tmp/data/input", exist_ok=True)
    url = "https://www.anatel.gov.br/dadosabertos/paineis_de_dados/acessos/acessos_banda_larga_fixa.zip"
    response = requests.get(url)

    with open("/tmp/data/input/acessos_banda_larga_fixa.zip", "wb") as zip_file:
        zip_file.write(response.content)

    with zipfile.ZipFile(
        "/tmp/data/input/acessos_banda_larga_fixa.zip", "r"
    ) as zip_ref:
        zip_ref.extractall("/tmp/data/input")

    pasta = "/tmp/data/input"
    banda_larga = os.path.join(pasta, "acessos_banda_larga_fixa.zip")

    # anos = ['2007-2010', '2011-2012', '2013-2014', '2015-2016', '2017-2018', '2019-2020', '2021', '2022', '2023']
    anos = ["2007-2010"]
    with ZipFile(banda_larga) as z:
        for ano in anos:

            with z.open(f"Acessos_Banda_Larga_Fixa_{ano}.csv") as f:
                df = pd.read_csv(f, sep=";", encoding="utf-8")
                df = check_and_create_column.run(df, "Tipo de Produto")

                # ! A partir do ano de 2021, há uma nova coluna chamada 'Tipo de Produto'
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

                # organização das variáveis
                df.drop(["grupo_economico", "municipio"], axis=1, inplace=True)

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

                df.replace(np.nan, "", inplace=True)
                df["transmissao"] = df["transmissao"].apply(
                    lambda x: x.replace("Cabo Metálico", "Cabo Metalico")
                )
                df["transmissao"] = df["transmissao"].apply(
                    lambda x: x.replace("Satélite", "Satelite")
                )
                df["transmissao"] = df["transmissao"].apply(
                    lambda x: x.replace("Híbrido", "Hibrido")
                )
                df["transmissao"] = df["transmissao"].apply(
                    lambda x: x.replace("Fibra Óptica", "Fibra Optica")
                )
                df["transmissao"] = df["transmissao"].apply(
                    lambda x: x.replace("Rádio", "Radio")
                )

                to_partitions.run(
                    df=df,
                    partition_columns=["ano", "mes", "sigla_uf"],
                    savepath="/tmp/data/microdados.csv",
                )

    return "/tmp/data/microdados.csv"
