"""
Tasks Prefect de br_bndes_operacoes_contratadas.

Envolvem as funcoes puras de utils.py em @task. As tasks compartilhadas
(upload_to_gcs, run_dbt, poll/commit source update) sao chamadas direto no
flow, nao aqui.

As tasks da operacoes_pre_embarque ficam no fim do modulo e chamam as funcoes
pelo modulo (`utils.download_table`), porque repetem os nomes delas.
"""

from datetime import datetime
from pathlib import Path

from prefect import task

from pipelines.constants import constants as global_constants
from pipelines.datasets.br_bndes_operacoes_contratadas import utils
from pipelines.datasets.br_bndes_operacoes_contratadas.constants import (
    TABLES_CONSTANTS,
    constants_administracao_publica,
    constants_exportacao_bens,
    constants_exportacao_servicos,
)
from pipelines.datasets.br_bndes_operacoes_contratadas.utils import (
    clean,
    clean_administracao_publica,
    clean_exportacao_bens,
    clean_exportacao_servicos,
    download_csv,
    get_source_last_modified,
)

TASK_RETRIES = global_constants.TASK_MAX_RETRIES.value
TASK_RETRY_DELAY_SECONDS = global_constants.TASK_RETRY_DELAY.value


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def get_source_max_date(table_id: str) -> datetime:
    """
    Le o last_modified do recurso no CKAN (sinal de atualizacao p/ o poll).

    Args:
        table_id (str): chave em TABLES_CONSTANTS
            (ex.: "operacoes_indiretas_automaticas").

    Returns:
        datetime: data/hora da ultima publicacao do CSV no portal.
    """
    table_constants = TABLES_CONSTANTS[table_id]
    return get_source_last_modified(
        table_constants.RESOURCE_SHOW_URL.value,
        table_constants.LAST_MODIFIED_FORMAT.value,
    )


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def download_source_csv(table_id: str) -> str:
    """
    Baixa o CSV consolidado da tabela para o INPUT_PATH do config.

    Args:
        table_id (str): chave em TABLES_CONSTANTS
            (ex.: "operacoes_indiretas_automaticas").

    Returns:
        str: caminho local do CSV baixado.
    """
    table_constants = TABLES_CONSTANTS[table_id]

    dest = (
        Path(table_constants.INPUT_PATH.value)
        / table_constants.CSV_FILENAME.value
    )

    dest_path: Path = download_csv(
        dest=dest, url=table_constants.DOWNLOAD_URL.value
    )

    return str(dest_path)


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def clean_and_partition(csv_path: str, table_id: str) -> str:
    """
    Limpa o CSV e grava Parquet particionado por ano.

    Args:
        csv_path (str): caminho do CSV baixado (saida de download_source_csv).
        table_id (str): chave em TABLES_CONSTANTS, repassada a clean()
            p/ resolver RENAME/ORDER_COLUMNS/SCHEMA da tabela.

    Returns:
        str: raiz das particoes gravadas (output_dir), p/ o upload_to_gcs.
    """
    table_constants = TABLES_CONSTANTS[table_id]

    output_dir = Path(table_constants.OUTPUT_PATH.value) / table_id

    clean(csv_path=Path(csv_path), output_dir=output_dir, table_id=table_id)

    return str(output_dir)


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def get_source_max_date_administracao_publica() -> datetime:
    """
    Le o last_modified do recurso CKAN de operacoes_administracao_publica (poll).

    Returns:
        datetime: data/hora da ultima publicacao do CSV no portal.
    """
    return get_source_last_modified(
        constants_administracao_publica.RESOURCE_SHOW_URL.value,
        constants_administracao_publica.LAST_MODIFIED_FORMAT.value,
    )


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def download_administracao_publica_csv() -> str:
    """
    Baixa o CSV de operacoes_administracao_publica para o INPUT_PATH.

    Returns:
        str: caminho local do CSV baixado.
    """
    dest = (
        Path(constants_administracao_publica.INPUT_PATH.value)
        / constants_administracao_publica.CSV_FILENAME.value
    )

    downloaded_file_path = download_csv(
        dest=dest, url=constants_administracao_publica.DOWNLOAD_URL.value
    )

    return str(downloaded_file_path)


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def clean_and_partition_administracao_publica(csv_path: str) -> str:
    """
    Limpa o CSV de operacoes_administracao_publica e grava Parquet por ano.

    Args:
        csv_path (str): caminho do CSV baixado.

    Returns:
        str: raiz das particoes gravadas (output_dir), p/ o upload_to_gcs.
    """
    output_dir = (
        Path(constants_administracao_publica.OUTPUT_PATH.value)
        / constants_administracao_publica.TABLE_ID.value
    )

    clean_administracao_publica(csv_path=Path(csv_path), output_dir=output_dir)

    return str(output_dir)


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def get_source_max_date_exportacao_bens() -> datetime:
    """
    Le o last_modified do recurso CKAN de operacoes_exportacao_bens (poll).

    Returns:
        datetime: data/hora da ultima publicacao do CSV no portal.
    """
    return get_source_last_modified(
        constants_exportacao_bens.RESOURCE_SHOW_URL.value,
        constants_exportacao_bens.LAST_MODIFIED_FORMAT.value,
    )


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def download_exportacao_bens_csv() -> str:
    """
    Baixa o CSV de operacoes_exportacao_bens para o INPUT_PATH.

    Returns:
        str: caminho local do CSV baixado.
    """
    dest = (
        Path(constants_exportacao_bens.INPUT_PATH.value)
        / constants_exportacao_bens.CSV_FILENAME.value
    )

    downloaded_file_path = download_csv(
        dest=dest, url=constants_exportacao_bens.DOWNLOAD_URL.value
    )

    return str(downloaded_file_path)


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def clean_and_partition_exportacao_bens(csv_path: str) -> str:
    """
    Limpa o CSV de operacoes_exportacao_bens e grava Parquet por ano.

    Args:
        csv_path (str): caminho do CSV baixado.

    Returns:
        str: raiz das particoes gravadas (output_dir), p/ o upload_to_gcs.
    """
    output_dir = (
        Path(constants_exportacao_bens.OUTPUT_PATH.value)
        / constants_exportacao_bens.TABLE_ID.value
    )

    clean_exportacao_bens(csv_path=Path(csv_path), output_dir=output_dir)

    return str(output_dir)


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def get_source_max_date_exportacao_servicos() -> datetime:
    """
    Le o last_modified do recurso CKAN de operacoes_exportacao_servicos (poll).

    Returns:
        datetime: data/hora da ultima publicacao do CSV no portal.
    """
    return get_source_last_modified(
        constants_exportacao_servicos.RESOURCE_SHOW_URL.value,
        constants_exportacao_servicos.LAST_MODIFIED_FORMAT.value,
    )


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def download_exportacao_servicos_csv() -> str:
    """
    Baixa o CSV de operacoes_exportacao_servicos para o INPUT_PATH.

    Sem validacao de tamanho: o /datastore/dump responde chunked, sem
    Content-Length, e ignora Range. A contraprova e a contagem de linhas do
    datastore, conferida no clean.

    Returns:
        str: caminho local do CSV baixado.
    """
    dest = (
        Path(constants_exportacao_servicos.INPUT_PATH.value)
        / constants_exportacao_servicos.CSV_FILENAME.value
    )

    downloaded_file_path = download_csv(
        dest=dest,
        url=constants_exportacao_servicos.DOWNLOAD_URL.value,
        validate_size=False,
    )

    return str(downloaded_file_path)


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def clean_and_partition_exportacao_servicos(csv_path: str) -> str:
    """
    Limpa o CSV de operacoes_exportacao_servicos e grava Parquet por ano.

    Args:
        csv_path (str): caminho do CSV baixado.

    Returns:
        str: raiz das particoes gravadas (output_dir), p/ o upload_to_gcs.
    """
    output_dir = (
        Path(constants_exportacao_servicos.OUTPUT_PATH.value)
        / constants_exportacao_servicos.TABLE_ID.value
    )

    clean_exportacao_servicos(csv_path=Path(csv_path), output_dir=output_dir)

    return str(output_dir)


@task(retries=3, retry_delay_seconds=30)
def get_source_last_modified_pre_embarque() -> str:
    """Lê a data da última republicação do recurso.

    Returns:
        A data do `last_modified`, no formato `%Y-%m-%d`.
    """
    return utils.get_source_last_modified_pre_embarque()


@task(retries=3, retry_delay_seconds=60)
def download_table() -> Path:
    """Baixa o CSV e confere o MD5.

    Returns:
        O caminho do CSV baixado.
    """
    return utils.download_table()


@task
def clean_table(csv_path: Path) -> Path:
    """Limpa e particiona a tabela.

    Args:
        csv_path: CSV baixado por `download_table`.

    Returns:
        O diretório particionado, no formato esperado por `upload_to_gcs`.
    """
    return utils.clean_table(csv_path=csv_path)
