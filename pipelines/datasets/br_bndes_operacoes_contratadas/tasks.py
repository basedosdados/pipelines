"""
Tasks Prefect de br_bndes_operacoes_contratadas.

Envolvem as funcoes puras de utils.py em @task. No fim do modulo ficam
`<tabela>_get_latest_update` e `<tabela>_extract_load_data`, que o flows.py
entrega ao CheckThenExtractLoadPipeline.

As tasks da operacoes_pre_embarque ficam depois das outras e chamam as funcoes
pelo modulo (`utils.download_table`), porque repetem os nomes delas.
"""

from datetime import date
from pathlib import Path

from prefect import task

from pipelines.constants import constants as global_constants
from pipelines.datasets.br_bndes_operacoes_contratadas import utils
from pipelines.datasets.br_bndes_operacoes_contratadas.constants import (
    TABLES_CONSTANTS,
    constants_administracao_publica,
    constants_exportacao_bens,
    constants_exportacao_servicos,
    constants_indiretas_automaticas,
    constants_nao_automaticas,
    constants_pre_embarque,
)
from pipelines.datasets.br_bndes_operacoes_contratadas.utils import (
    assert_md5,
    clean,
    clean_administracao_publica,
    clean_exportacao_bens,
    clean_exportacao_servicos,
    download_csv,
    get_source_last_modified,
    get_source_md5,
)
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection

TASK_RETRIES = global_constants.TASK_MAX_RETRIES.value
TASK_RETRY_DELAY_SECONDS = global_constants.TASK_RETRY_DELAY.value


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def get_source_max_date(table_id: str) -> date:
    """
    Le o last_modified do recurso no CKAN (sinal de atualizacao p/ o poll).

    Args:
        table_id (str): chave em TABLES_CONSTANTS
            (ex.: "operacoes_indiretas_automaticas").

    Returns:
        date: dia da ultima publicacao do CSV no portal.
    """
    table_constants = TABLES_CONSTANTS[table_id]
    return get_source_last_modified(
        table_constants.RESOURCE_SHOW_URL.value,
        table_constants.LAST_MODIFIED_FORMAT.value,
    )


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def download_source_csv(table_id: str) -> str:
    """
    Baixa o CSV consolidado da tabela para o INPUT_PATH do config e confere
    o MD5 contra o result.hash do recurso.

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

    assert_md5(
        dest_path,
        get_source_md5(table_constants.RESOURCE_SHOW_URL.value),
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
def get_source_max_date_administracao_publica() -> date:
    """
    Le o last_modified do recurso CKAN de operacoes_administracao_publica (poll).

    Returns:
        date: dia da ultima publicacao do CSV no portal.
    """
    return get_source_last_modified(
        constants_administracao_publica.RESOURCE_SHOW_URL.value,
        constants_administracao_publica.LAST_MODIFIED_FORMAT.value,
    )


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def download_administracao_publica_csv() -> str:
    """
    Baixa o CSV de operacoes_administracao_publica para o INPUT_PATH e
    confere o MD5 contra o result.hash do recurso.

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

    assert_md5(
        downloaded_file_path,
        get_source_md5(
            constants_administracao_publica.RESOURCE_SHOW_URL.value
        ),
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
def get_source_max_date_exportacao_bens() -> date:
    """
    Le o last_modified do recurso CKAN de operacoes_exportacao_bens (poll).

    Returns:
        date: dia da ultima publicacao do CSV no portal.
    """
    return get_source_last_modified(
        constants_exportacao_bens.RESOURCE_SHOW_URL.value,
        constants_exportacao_bens.LAST_MODIFIED_FORMAT.value,
    )


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def download_exportacao_bens_csv() -> str:
    """
    Baixa o CSV de operacoes_exportacao_bens para o INPUT_PATH e confere o
    MD5 contra o result.hash do recurso.

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

    assert_md5(
        downloaded_file_path,
        get_source_md5(constants_exportacao_bens.RESOURCE_SHOW_URL.value),
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
def get_source_max_date_exportacao_servicos() -> date:
    """
    Le o last_modified do recurso CKAN de operacoes_exportacao_servicos (poll).

    Returns:
        date: dia da ultima publicacao do CSV no portal.
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
def get_source_last_modified_pre_embarque() -> date:
    """Lê o dia da última republicação do recurso.

    Returns:
        O dia do `last_modified` do recurso.
    """
    return get_source_last_modified(
        constants_pre_embarque.RESOURCE_SHOW_URL.value,
        constants_pre_embarque.LAST_MODIFIED_FORMAT.value,
    )


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


def indiretas_automaticas_get_latest_update() -> SourceInspection:
    """Etapa check_update da operacoes_indiretas_automaticas.

    Returns:
        O dia da última publicação do recurso, comparado com o Table.Update.
    """
    return SourceInspection(
        reference_date=get_source_max_date(
            table_id=constants_indiretas_automaticas.TABLE_ID.value
        ),
        compare_against="table_update",
    )


def indiretas_automaticas_extract_load_data(
    download_params: dict,
) -> ExtractAndLoad:
    """Etapa extract_and_load da operacoes_indiretas_automaticas.

    Args:
        download_params: Repassado pelo check_update; não é usado.

    Returns:
        A pasta particionada por `ano` e a cobertura da tabela.
    """
    table_id = constants_indiretas_automaticas.TABLE_ID.value
    csv_path = download_source_csv(table_id=table_id)
    output_dir = clean_and_partition(csv_path=csv_path, table_id=table_id)
    return ExtractAndLoad(
        coverage=constants_indiretas_automaticas.COVERAGE.value.model_dump(),
        data_path=output_dir,
        dump_mode="overwrite",
        source_format="parquet",
    )


def nao_automaticas_get_latest_update() -> SourceInspection:
    """Etapa check_update da operacoes_nao_automaticas.

    Returns:
        O dia da última publicação do recurso, comparado com o Table.Update.
    """
    return SourceInspection(
        reference_date=get_source_max_date(
            table_id=constants_nao_automaticas.TABLE_ID.value
        ),
        compare_against="table_update",
    )


def nao_automaticas_extract_load_data(
    download_params: dict,
) -> ExtractAndLoad:
    """Etapa extract_and_load da operacoes_nao_automaticas.

    Args:
        download_params: Repassado pelo check_update; não é usado.

    Returns:
        A pasta particionada por `ano` e a cobertura da tabela.
    """
    table_id = constants_nao_automaticas.TABLE_ID.value
    csv_path = download_source_csv(table_id=table_id)
    output_dir = clean_and_partition(csv_path=csv_path, table_id=table_id)
    return ExtractAndLoad(
        coverage=constants_nao_automaticas.COVERAGE.value.model_dump(),
        data_path=output_dir,
        dump_mode="overwrite",
        source_format="parquet",
    )


def administracao_publica_get_latest_update() -> SourceInspection:
    """Etapa check_update da operacoes_administracao_publica.

    Returns:
        O dia da última publicação do recurso, comparado com o Table.Update.
    """
    return SourceInspection(
        reference_date=get_source_max_date_administracao_publica(),
        compare_against="table_update",
    )


def administracao_publica_extract_load_data(
    download_params: dict,
) -> ExtractAndLoad:
    """Etapa extract_and_load da operacoes_administracao_publica.

    Args:
        download_params: Repassado pelo check_update; não é usado.

    Returns:
        A pasta particionada por `ano` e a cobertura da tabela.
    """
    csv_path = download_administracao_publica_csv()
    output_dir = clean_and_partition_administracao_publica(csv_path=csv_path)
    return ExtractAndLoad(
        coverage=constants_administracao_publica.COVERAGE.value.model_dump(),
        data_path=output_dir,
        dump_mode="overwrite",
        source_format="parquet",
    )


def exportacao_bens_get_latest_update() -> SourceInspection:
    """Etapa check_update da operacoes_exportacao_bens.

    Returns:
        O dia da última publicação do recurso, comparado com o Table.Update.
    """
    return SourceInspection(
        reference_date=get_source_max_date_exportacao_bens(),
        compare_against="table_update",
    )


def exportacao_bens_extract_load_data(
    download_params: dict,
) -> ExtractAndLoad:
    """Etapa extract_and_load da operacoes_exportacao_bens.

    Args:
        download_params: Repassado pelo check_update; não é usado.

    Returns:
        A pasta particionada por `ano` e a cobertura da tabela.
    """
    csv_path = download_exportacao_bens_csv()
    output_dir = clean_and_partition_exportacao_bens(csv_path=csv_path)
    return ExtractAndLoad(
        coverage=constants_exportacao_bens.COVERAGE.value.model_dump(),
        data_path=output_dir,
        dump_mode="overwrite",
        source_format="parquet",
    )


def exportacao_servicos_get_latest_update() -> SourceInspection:
    """Etapa check_update da operacoes_exportacao_servicos.

    Returns:
        O dia da última publicação do recurso, comparado com o Table.Update.
    """
    return SourceInspection(
        reference_date=get_source_max_date_exportacao_servicos(),
        compare_against="table_update",
    )


def exportacao_servicos_extract_load_data(
    download_params: dict,
) -> ExtractAndLoad:
    """Etapa extract_and_load da operacoes_exportacao_servicos.

    Args:
        download_params: Repassado pelo check_update; não é usado.

    Returns:
        A pasta particionada por `ano` e a cobertura da tabela.
    """
    csv_path = download_exportacao_servicos_csv()
    output_dir = clean_and_partition_exportacao_servicos(csv_path=csv_path)
    return ExtractAndLoad(
        coverage=constants_exportacao_servicos.COVERAGE.value.model_dump(),
        data_path=output_dir,
        dump_mode="overwrite",
        source_format="parquet",
    )


def pre_embarque_get_latest_update() -> SourceInspection:
    """Etapa check_update da operacoes_pre_embarque.

    Returns:
        O dia da última publicação do recurso, comparado com o Table.Update.
    """
    return SourceInspection(
        reference_date=get_source_last_modified_pre_embarque(),
        compare_against="table_update",
    )


def pre_embarque_extract_load_data(
    download_params: dict,
) -> ExtractAndLoad:
    """Etapa extract_and_load da operacoes_pre_embarque.

    Args:
        download_params: Repassado pelo check_update; não é usado.

    Returns:
        A pasta particionada por `ano` e a cobertura da tabela.
    """
    csv_path = download_table()
    output_dir = clean_table(csv_path=csv_path)
    return ExtractAndLoad(
        coverage=constants_pre_embarque.COVERAGE.value.model_dump(),
        data_path=str(output_dir),
        dump_mode="overwrite",
        source_format="parquet",
    )
