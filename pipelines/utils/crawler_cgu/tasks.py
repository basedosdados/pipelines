# -*- coding: utf-8 -*-
"""
Tasks for br_cgu_cartao_pagamento
"""
from datetime import datetime
from prefect import task
import os
import basedosdados as bd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import polars as pl
import pandas as pd
import dask.dataframe as dd
from tqdm import tqdm
import gc
from dateutil.relativedelta import relativedelta
from pipelines.utils.utils import log, to_partitions, download_and_unzip_file
from pipelines.utils.metadata.utils import get_api_most_recent_date, get_url
from pipelines.utils.crawler_cgu.utils import (
    read_csv,
    last_date_in_metadata,
    read_and_clean_csv,
    build_urls,
    partition_data_beneficios_cidadao
)
from pipelines.utils.crawler_cgu.constants import constants
from pipelines.utils.crawler_cgu.utils import download_file
from typing import Tuple


@task
def partition_data(table_id: str, dataset_id : str) -> str:
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

    if dataset_id in ["br_cgu_cartao_pagamento", "br_cgu_licitacao_contrato", "br_cgu_beneficios_cidadao"]:
        log("---------------------------- Read data ----------------------------")
        df = read_csv(dataset_id = dataset_id, table_id = table_id)
        if dataset_id == "br_cgu_cartao_pagamento:":
            log(" ---------------------------- Partiting data -----------------------")
            to_partitions(
                data = df,
                partition_columns=['ANO_EXTRATO', 'MES_EXTRATO'],
                savepath = constants.TABELA.value[table_id]['OUTPUT'],
                file_type='csv')

            log("---------------------------- Data partitioned ----------------------")
            return constants.TABELA.value[table_id]['OUTPUT']

        if dataset_id == "br_cgu_licitacao_contrato":
            log(" ---------------------------- Partiting data -----------------------")
            to_partitions(
                data=df,
                partition_columns=["ano", "mes"],
                savepath=constants.TABELA_LICITACAO_CONTRATO.value[table_id]["OUTPUT"],
                file_type="csv",
            )
            log("---------------------------- Data partitioned ----------------------")
            return constants.TABELA_LICITACAO_CONTRATO.value[table_id]["OUTPUT"]

    elif dataset_id == "br_cgu_servidores_executivo_federal":

        log("---------------------------- Read data ----------------------------")
        df = read_and_clean_csv(table_id = table_id)
        log(" ---------------------------- Partiting data -----------------------")
        to_partitions(
            data=df,
            partition_columns=["ano", "mes"],
            savepath=constants.TABELA_SERVIDORES.value[table_id]['OUTPUT'],
        )
        log("---------------------------- Data partitioned ----------------------")
        return constants.TABELA_SERVIDORES.value[table_id]['OUTPUT']


@task
# https://stackoverflow.com/questions/26124417/how-to-convert-a-csv-file-to-parquet
def read_and_partition_beneficios_cidadao(table_id):
    constants_cgu_beneficios_cidadao = constants.TABELA_BENEFICIOS_CIDADAO.value[table_id]
    for nome_arquivo in os.listdir(constants_cgu_beneficios_cidadao['INPUT']):
        if nome_arquivo.endswith(".csv"):
            log(f"Carregando o arquivo: {nome_arquivo}")

            parquet_file = f"{constants_cgu_beneficios_cidadao['INPUT']}{nome_arquivo.replace('.csv', '.parquet')}"
            parquet_writer = None
            with pd.read_csv(
                f"{constants_cgu_beneficios_cidadao['INPUT']}{nome_arquivo}",
                sep=";",
                encoding="latin-1",
                chunksize=100000,
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
                for chunk in tqdm(reader):
                    chunk.rename(
                        columns=(
                            constants.RENAMER_NOVO_BOLSA_FAMILIA.value
                            if table_id == "novo_bolsa_familia"
                            else (
                                constants.RENAMER_GARANTIA_SAFRA.value
                                if table_id == "garantia_safra"
                                else constants.RENAMER_BPC.value
                            )
                        ),
                        inplace=True,
                    )
                    if parquet_writer is None:
                        parquet_schema = pa.Table.from_pandas(chunk).schema
                        parquet_writer = pq.ParquetWriter(parquet_file, parquet_schema, compression='snappy')

                    # Escrever diretamente o chunk no arquivo parquet
                    table = pa.Table.from_pandas(chunk, schema=parquet_schema)
                    parquet_writer.write_table(table)

                    del chunk
                    gc.collect()

            if parquet_writer:
                parquet_writer.close()

            log(f"Arquivo parquet criado: {parquet_file}")
    log("Abrindo arquivo parquet: {parquet_file}")
    df = pl.read_parquet(parquet_file)

    log(f"---------------------------- Partition Data: {table_id=} ----------------------------")
    if table_id == "novo_bolsa_familia":
        partition_data_beneficios_cidadao(table_id, df, "mes_competencia", "sigla_uf")

    elif table_id == "bpc":
        partition_data_beneficios_cidadao(table_id, df, "mes_competencia")

    elif table_id == "garantia_safra":
        partition_data_beneficios_cidadao(table_id, df, "mes_competencia", "sigla_uf")
        to_partitions(
            df,
            partition_columns=["mes_referencia"],
            savepath=constants.TABELA_BENEFICIOS_CIDADAO.value[table_id]['OUTPUT'],
            file_type="parquet",
        )
        log(constants.TABELA_BENEFICIOS_CIDADAO.value[table_id]['OUTPUT'])
    log("Partição feita.")

    del chunk
    gc.collect()

    return constants.TABELA_BENEFICIOS_CIDADAO.value[table_id]['OUTPUT']



@task
def get_current_date_and_download_file(table_id : str,
                                        dataset_id : str,
                                        relative_month : int = 1) -> datetime:
    """
    Get the maximum date from a given table for a specific year and month.

    Args:
        table_id (str): The ID of the table.
        year (int): The year.
        month (int): The month.

    Returns:
        datetime: The maximum date as a datetime object.
    """
    last_date_in_api, next_date_in_api = last_date_in_metadata(
                                    dataset_id = dataset_id,
                                    table_id = table_id,
                                    relative_month = relative_month
                                    )
    log(f"Last date in API: {last_date_in_api}")
    log(f"Next date in API: {next_date_in_api}")

    max_date = str(download_file(table_id = table_id,
                                dataset_id = dataset_id,
                                year = next_date_in_api.year,
                                month = next_date_in_api.month,
                                relative_month=relative_month))

    log(f"Max date: {max_date}")

    date = datetime.strptime(max_date, '%Y-%m-%d')

    return date


@task
def verify_all_url_exists_to_download(dataset_id, table_id, relative_month) -> bool:
    """
    Verifies if all URLs are valid and can be downloaded.

    Args:
        table_id (str): The identifier for the table to download data for.
        year (int): The year for which data is to be downloaded.
        month (int): The month for which data is to be downloaded.

    Returns:
        bool: True if all URLs are valid and can be downloaded, False otherwise.
    """
    last_date_in_api, next_date_in_api = last_date_in_metadata(
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
        if requests.get(url).status_code != 200:
            log(f"A URL {url=} não existe!")
            return False

        log(f"A URL {url=} existe!")
    return True

@task
def dict_for_table(table_id: str) -> dict:

    DICT_FOR_TABLE = {
        "novo_bolsa_familia": {"year": "ano_competencia", "month": "mes_competencia"},
        "safra_garantia": {"year": "ano_referencia", "month": "mes_referencia"},
        "bpc": {"year": "ano_competencia", "month": "mes_competencia"},
    }

    return DICT_FOR_TABLE[table_id]