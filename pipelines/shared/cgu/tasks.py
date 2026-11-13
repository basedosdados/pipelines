"""
Tasks compartilhadas para os datasets br_cgu_* — Prefect 3.
"""

import gc
import os
from datetime import date

import pandas as pd
from prefect import task
from prefect.tasks import exponential_backoff
from tqdm import tqdm

from pipelines.constants import constants as global_constants
from pipelines.shared.cgu.constants import (
    BENEFICIOS_CIDADAO_DATASET_ID,
    BROWSERS_USER_AGENT,
    CARTAO_PAGAMENTO_DATASET_ID,
    DOWNLOAD_RETRIES,
    DOWNLOAD_RETRY_BACKOFF_SECONDS,
    DOWNLOAD_RETRY_JITTER,
    DTYPES_BPC,
    DTYPES_GARANTIA_SAFRA,
    DTYPES_NOVO_BOLSA_FAMILIA,
    LICITACAO_CONTRATO_DATASET_ID,
    RENAMER_BPC,
    RENAMER_GARANTIA_SAFRA,
    RENAMER_NOVO_BOLSA_FAMILIA,
    SERVIDORES_DATASET_ID,
    TABELAS,
)
from pipelines.shared.cgu.utils import (
    build_input,
    build_urls,
    is_transient_download_error,
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
    if dataset_id in [
        CARTAO_PAGAMENTO_DATASET_ID,
        LICITACAO_CONTRATO_DATASET_ID,
    ]:
        df = read_csv(
            dataset_id=dataset_id, table_id=table_id, column_replace=None
        )
        if dataset_id == CARTAO_PAGAMENTO_DATASET_ID:
            to_partitions(
                data=df,
                partition_columns=["ANO_EXTRATO", "MES_EXTRATO"],
                savepath=TABELAS[dataset_id][table_id]["OUTPUT"],
                file_type="csv",
            )
            return TABELAS[dataset_id][table_id]["OUTPUT"]

        if dataset_id == LICITACAO_CONTRATO_DATASET_ID:
            to_partitions(
                data=df,
                partition_columns=["ano", "mes"],
                savepath=TABELAS[dataset_id][table_id]["OUTPUT"],
                file_type="csv",
            )
            return TABELAS[dataset_id][table_id]["OUTPUT"]

    elif dataset_id == SERVIDORES_DATASET_ID:
        df = read_and_clean_csv(table_id=table_id)
        to_partitions(
            data=df,
            partition_columns=["ano", "mes"],
            savepath=TABELAS[dataset_id][table_id]["OUTPUT"],
        )
        return TABELAS[dataset_id][table_id]["OUTPUT"]


def _retry_if_transient(task, task_run, state) -> bool:
    """`retry_condition_fn` do download: só repete erro transitório."""
    return is_transient_download_error(state.result(raise_on_failure=False))


@task(
    retries=DOWNLOAD_RETRIES,
    retry_delay_seconds=exponential_backoff(
        backoff_factor=DOWNLOAD_RETRY_BACKOFF_SECONDS
    ),
    retry_jitter_factor=DOWNLOAD_RETRY_JITTER,
    retry_condition_fn=_retry_if_transient,
)
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

    value_constants = TABELAS[dataset_id][table_id]
    built_urls = build_urls(
        dataset_id=dataset_id,
        table_id=table_id,
        # pyrefly: ignore [bad-argument-type]
        url=value_constants["URL"],
        year=reference_date.year,
        month=reference_date.month,
    )
    if dataset_id in [
        CARTAO_PAGAMENTO_DATASET_ID,
        LICITACAO_CONTRATO_DATASET_ID,
        BENEFICIOS_CIDADAO_DATASET_ID,
    ]:
        destino = value_constants["INPUT"]
        # pyrefly: ignore [bad-argument-type]
        os.makedirs(destino, exist_ok=True)

        if dataset_id == BENEFICIOS_CIDADAO_DATASET_ID and isinstance(
            built_urls, str
        ):
            built_urls = built_urls.rstrip("/")
            headers = {"User-Agent": BROWSERS_USER_AGENT["chrome"]}
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

    elif dataset_id == SERVIDORES_DATASET_ID:
        input_dirs = build_input(table_id)
        headers = {"User-Agent": BROWSERS_USER_AGENT["chrome"]}
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
    constants_cgu_beneficios_cidadao = TABELAS[BENEFICIOS_CIDADAO_DATASET_ID][
        table_id
    ]
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
                        DTYPES_NOVO_BOLSA_FAMILIA
                        if table_id == "novo_bolsa_familia"
                        else (
                            DTYPES_GARANTIA_SAFRA
                            if table_id == "garantia_safra"
                            else DTYPES_BPC
                        )
                    ),
                ) as reader:
                    for number, chunk in enumerate(tqdm(reader)):
                        chunk = chunk.rename(
                            columns=(
                                RENAMER_NOVO_BOLSA_FAMILIA
                                if table_id == "novo_bolsa_familia"
                                else (
                                    RENAMER_GARANTIA_SAFRA
                                    if table_id == "garantia_safra"
                                    else RENAMER_BPC
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
