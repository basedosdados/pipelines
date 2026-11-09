"""
Tasks compartilhadas para os datasets br_cgu_* — Prefect 3.
"""

import gc
import os
from datetime import date

import pandas as pd
from prefect import task
from tqdm import tqdm

from pipelines.constants import constants as global_constants
from pipelines.shared.cgu.constants import constants
from pipelines.shared.cgu.utils import (
    build_input,
    build_urls,
    partition_data_beneficios_cidadao,
    read_and_clean_csv,
    read_csv,
)
from pipelines.utils.utils import download_and_unzip_file, log, to_partitions

TASK_RETRIES = global_constants.TASK_MAX_RETRIES.value
TASK_RETRY_DELAY_SECONDS = global_constants.TASK_RETRY_DELAY.value


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
# pyrefly: ignore [bad-return]
def partition_data(table_id: str, dataset_id: str) -> str:
    log("---------------------------- Read data ----------------------------")
    if dataset_id in ["br_cgu_cartao_pagamento", "br_cgu_licitacao_contrato"]:
        df = read_csv(
            dataset_id=dataset_id, table_id=table_id, column_replace=None
        )
        if dataset_id == "br_cgu_cartao_pagamento":
            to_partitions(
                data=df,
                partition_columns=["ANO_EXTRATO", "MES_EXTRATO"],
                savepath=constants.TABELAS.value[dataset_id][table_id][
                    "OUTPUT"
                ],
                file_type="csv",
            )
            return constants.TABELAS.value[dataset_id][table_id]["OUTPUT"]

        if dataset_id == "br_cgu_licitacao_contrato":
            to_partitions(
                data=df,
                partition_columns=["ano", "mes"],
                savepath=constants.TABELAS.value[dataset_id][table_id][
                    "OUTPUT"
                ],
                file_type="csv",
            )
            return constants.TABELAS.value[dataset_id][table_id]["OUTPUT"]

    elif dataset_id == "br_cgu_servidores_executivo_federal":
        df = read_and_clean_csv(table_id=table_id)
        to_partitions(
            data=df,
            partition_columns=["ano", "mes"],
            savepath=constants.TABELAS.value[dataset_id][table_id]["OUTPUT"],
        )
        return constants.TABELAS.value[dataset_id][table_id]["OUTPUT"]


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def download_file(
    dataset_id: str,
    table_id: str,
    reference_date: date | str,
) -> None:
    """Baixa e extrai os arquivos de um mês de referência já definido.

    Args:
        dataset_id: ID do dataset.
        table_id: ID da tabela.
        reference_date: Mês a baixar (usa apenas ano e mês). Aceita `date`
            ou string ISO (`YYYY-MM-DD`), que é como o stage_dispatch a
            entrega em `download_params`.
    """
    if isinstance(reference_date, str):
        reference_date = date.fromisoformat(reference_date)

    value_constants = constants.TABELAS.value[dataset_id][table_id]
    built_urls = build_urls(
        dataset_id=dataset_id,
        table_id=table_id,
        # pyrefly: ignore [bad-argument-type]
        url=value_constants["URL"],
        year=reference_date.year,
        month=reference_date.month,
    )
    if dataset_id in [
        "br_cgu_cartao_pagamento",
        "br_cgu_licitacao_contrato",
        "br_cgu_beneficios_cidadao",
    ]:
        destino = value_constants["INPUT"]
        # pyrefly: ignore [bad-argument-type]
        os.makedirs(destino, exist_ok=True)

        if dataset_id == "br_cgu_beneficios_cidadao" and isinstance(
            built_urls, str
        ):
            built_urls = built_urls.rstrip("/")
            headers = {
                "User-Agent": constants.BROWSERS_USER_AGENT.value["chrome"]
            }
        else:
            headers = None

        log(f"------------------ URL = {built_urls} ------------------")
        try:
            download_and_unzip_file(
                # pyrefly: ignore [bad-argument-type]
                built_urls,
                # pyrefly: ignore [bad-argument-type]
                destino,
                headers=headers,
            )
        except Exception as e:
            log(
                f"Não foi possível baixar: {built_urls}\nErro: {e}",
                level="error",
            )
            raise

    elif dataset_id == "br_cgu_servidores_executivo_federal":
        input_dirs = build_input(table_id)
        headers = {"User-Agent": constants.BROWSERS_USER_AGENT.value["chrome"]}
        for url, input_dir in zip(built_urls, input_dirs, strict=True):
            destino = f"{value_constants['INPUT']}/{input_dir}"
            log(f"------------------ URL = {url} ------------------")
            try:
                download_and_unzip_file(url, destino, headers=headers)
            except Exception as e:
                log(
                    f"Não foi possível baixar: {url}\nErro: {e}", level="error"
                )
                raise


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
# pyrefly: ignore [bad-return]
def read_and_partition_beneficios_cidadao(table_id: str) -> str:
    constants_cgu_beneficios_cidadao = constants.TABELAS.value[
        "br_cgu_beneficios_cidadao"
    ][table_id]
    for nome_arquivo in os.listdir(constants_cgu_beneficios_cidadao["INPUT"]):
        for nome_arquivo in os.listdir(
            constants_cgu_beneficios_cidadao["INPUT"]
        ):
            if nome_arquivo.endswith(".csv"):
                log(f"Carregando o arquivo: {nome_arquivo}")

                # pyrefly: ignore [no-matching-overload]
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
