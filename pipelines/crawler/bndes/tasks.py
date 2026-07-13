"""
Tasks Prefect do crawler br_bndes_operacoes_contratadas.

Envolvem as funcoes puras de utils.py em @task. As tasks compartilhadas
(upload_to_gcs, run_dbt, poll/commit source update) sao chamadas direto no
flow, nao aqui.
"""

from datetime import datetime
from pathlib import Path

from prefect import task

from pipelines.constants import constants as global_constants
from pipelines.crawler.bndes.constants import constants
from pipelines.crawler.bndes.utils import (
    clean,
    download_csv,
    get_source_last_modified,
)

TASK_RETRIES = global_constants.TASK_MAX_RETRIES.value
TASK_RETRY_DELAY_SECONDS = global_constants.TASK_RETRY_DELAY.value


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def get_source_max_date() -> datetime:
    """
    Le o last_modified do recurso no CKAN (sinal de atualizacao p/ o poll).

    Returns:
        datetime: data/hora da ultima publicacao do CSV no portal.
    """
    return get_source_last_modified()


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def download_source_csv() -> str:
    """
    Baixa o CSV consolidado para constants.INPUT_PATH.

    Returns:
        str: caminho local do CSV baixado.
    """
    dest = Path(constants.INPUT_PATH.value) / constants.CSV_FILENAME.value

    dest_path: Path = download_csv(dest)

    return str(dest_path)


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def clean_and_partition(csv_path: str) -> str:
    """
    Limpa o CSV e grava Parquet particionado por ano.

    Args:
        csv_path (str): caminho do CSV baixado (saida de download_source_csv).

    Returns:
        str: raiz das particoes gravadas (output_dir), p/ o upload_to_gcs.
    """

    output_dir = Path(constants.OUTPUT_PATH.value) / constants.TABLE_ID.value

    clean(csv_path=Path(csv_path), output_dir=output_dir)

    return str(output_dir)
