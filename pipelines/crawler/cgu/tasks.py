"""
Tasks for br_cgu_cartao_pagamento
"""

import gc
import os
from datetime import datetime

import pandas as pd
import requests
from prefect import task
from tqdm import tqdm

from pipelines.crawler.cgu.constants import constants
from pipelines.crawler.cgu.utils import (
    build_urls,
    download_file,
    last_date_in_metadata,
    partition_data_beneficios_cidadao,
    read_and_clean_csv,
    read_csv,
)
from pipelines.utils.utils import log, to_partitions


@task
def partition_data(table_id: str, dataset_id: str) -> str:
    """
    Partition data from a given table.

    This function reads data from a specified table, partitions it based on
    the columns 'ANO_EXTRATO' and 'MES_EXTRATO', and saves the partitioned
    data to a specified output path.

    Args:
        table_id (str): The identifier of the table to be partitioned.

    Returns:
        str: The path where the partitioned data is saved.
    """
    log("---------------------------- Read data ----------------------------")
    if dataset_id in ["br_cgu_cartao_pagamento", "br_cgu_licitacao_contrato"]:
        df = read_csv(
            dataset_id=dataset_id, table_id=table_id, column_replace=None
        )
        if dataset_id == "br_cgu_cartao_pagamento":
            to_partitions(
                data=df,
                partition_columns=["ANO_EXTRATO", "MES_EXTRATO"],
                savepath=constants.TABELA.value[table_id]["OUTPUT"],
                file_type="csv",
            )

            return constants.TABELA.value[table_id]["OUTPUT"]

        if dataset_id == "br_cgu_licitacao_contrato":
            to_partitions(
                data=df,
                partition_columns=["ano", "mes"],
                savepath=constants.TABELA_LICITACAO_CONTRATO.value[table_id][
                    "OUTPUT"
                ],
                file_type="csv",
            )

            return constants.TABELA_LICITACAO_CONTRATO.value[table_id][
                "OUTPUT"
            ]

    elif dataset_id == "br_cgu_servidores_executivo_federal":
        df = read_and_clean_csv(table_id=table_id)

        to_partitions(
            data=df,
            partition_columns=["ano", "mes"],
            savepath=constants.TABELA_SERVIDORES.value[table_id]["OUTPUT"],
        )

        return constants.TABELA_SERVIDORES.value[table_id]["OUTPUT"]


@task
# https://stackoverflow.com/questions/26124417/how-to-convert-a-csv-file-to-parquet
def read_and_partition_beneficios_cidadao(table_id):
    """
    Carrega arquivos CSV, realiza transformações e cria partições em um formato específico, retornando o caminho de saída.

    Parâmetros:
    - path (str): O caminho para os arquivos a serem processados.
    - table (str): O nome da tabela (possíveis valores: "novo_bolsa_familia", "garantia_safra", "bpc").

    Retorna:
    - str: O caminho do diretório de saída onde as partições foram criadas.

    Exemplo de uso:
    output_path = parquet_partition("/caminho/para/arquivos/", "novo_bolsa_familia")
    """
    constants_cgu_beneficios_cidadao = (
        constants.TABELA_BENEFICIOS_CIDADAO.value[table_id]
    )
    for nome_arquivo in os.listdir(constants_cgu_beneficios_cidadao["INPUT"]):
        for nome_arquivo in os.listdir(
            constants_cgu_beneficios_cidadao["INPUT"]
        ):
            if nome_arquivo.endswith(".csv"):
                log(f"Carregando o arquivo: {nome_arquivo}")

                with pd.read_csv(
                    f"{constants_cgu_beneficios_cidadao['INPUT']}{nome_arquivo}",
                    sep=";",
                    encoding="latin-1",
                    chunksize=500000,
                    decimal=",",
                    na_values="" if table_id != "bpc" else None,
                    dtype=(
                        constants.DTYPES_NOVO_BOLSA_FAMILIA.value
                        if table_id == "novo_bolsa_familia"
                        else (
                            constants.DTYPES_GARANTIA_SAFRA.value
                            if table_id == "garantia_safra"
                            else constants.DTYPES_BPC.value
                        )
                    ),
                ) as reader:
                    for number, chunk in enumerate(tqdm(reader)):
                        chunk = chunk.rename(
                            columns=(
                                constants.RENAMER_NOVO_BOLSA_FAMILIA.value
                                if table_id == "novo_bolsa_familia"
                                else (
                                    constants.RENAMER_GARANTIA_SAFRA.value
                                    if table_id == "garantia_safra"
                                    else constants.RENAMER_BPC.value
                                )
                            )
                        )
                        os.makedirs(
                            constants_cgu_beneficios_cidadao["OUTPUT"],
                            exist_ok=True,
                        )
                        log(f"Chunk {number} carregando.")
                        if table_id == "novo_bolsa_familia":
                            partition_data_beneficios_cidadao(
                                table_id=table_id,
                                df=chunk,
                                coluna1="mes_competencia",
                                coluna2="sigla_uf",
                                counter=number,
                            )
                        elif table_id == "bpc":
                            to_partitions(
                                data=chunk,
                                partition_columns=["mes_competencia"],
                                savepath=constants_cgu_beneficios_cidadao[
                                    "OUTPUT"
                                ],
                                file_type="csv",
                            )
                        else:
                            to_partitions(
                                data=chunk,
                                partition_columns=["mes_referencia"],
                                savepath=constants_cgu_beneficios_cidadao[
                                    "OUTPUT"
                                ],
                                file_type="parquet",
                            )

                        del chunk
                        gc.collect()

                    log("Partição feita.")

                return constants_cgu_beneficios_cidadao["OUTPUT"]


@task
def get_current_date_and_download_file(
    table_id: str, dataset_id: str, relative_month: int = 1
) -> datetime:
    """
    Get the maximum date from a given table for a specific year and month.

    Args:
        table_id (str): The ID of the table.
        year (int): The year.
        month (int): The month.

    Returns:
        datetime: The maximum date as a datetime object.
    """
    max_date = str(
        download_file(
            table_id=table_id,
            dataset_id=dataset_id,
            relative_month=relative_month,
        )
    )

    date = datetime.strptime(max_date, "%Y-%m-%d")

    return date


@task
def verify_all_url_exists_to_download(
    dataset_id, table_id, relative_month
) -> bool:
    """
    Verifies if all URLs are valid and can be downloaded.

    Args:
        table_id (str): The identifier for the table to download data for.
        year (int): The year for which data is to be downloaded.
        month (int): The month for which data is to be downloaded.

    Returns:
        bool: True if all URLs are valid and can be downloaded, False otherwise.
    """
    _, next_date_in_api = last_date_in_metadata(
        dataset_id=dataset_id, table_id=table_id, relative_month=relative_month
    )

    urls = build_urls(
        dataset_id,
        constants.URL_SERVIDORES.value,
        next_date_in_api.year,
        next_date_in_api.month,
        table_id,
    )

    for url in urls:
        log(f"Verificando se a URL {url=} existe")
        r = requests.get(url)
        if r.status_code != 200:
            log(f"A URL {url=} não existe!")
            return False

        else:
            log(f"A URL {url=} existe!")
            return True


@task
def dict_for_table(table_id: str) -> dict:
    dict_for_table = {
        "novo_bolsa_familia": {
            "year": "ano_competencia",
            "month": "mes_competencia",
        },
        "garantia_safra": {
            "year": "ano_referencia",
            "month": "mes_referencia",
        },
        "bpc": {"year": "ano_competencia", "month": "mes_competencia"},
    }

    return dict_for_table[table_id]
