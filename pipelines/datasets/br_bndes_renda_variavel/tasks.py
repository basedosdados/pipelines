"""
Tasks de br_bndes_renda_variavel.

As `@task` embrulham as funções de `utils.py`, onde fica a lógica. Abaixo delas
ficam as duas etapas que o conjunto escreve, uma casca por tabela, porque o
pipeline chama cada etapa sem argumentos ou só com `download_params`.
"""

from datetime import date
from pathlib import Path

from prefect import task

from pipelines.datasets.br_bndes_renda_variavel import utils
from pipelines.datasets.br_bndes_renda_variavel.constants import (
    DESEMBOLSO_TABLE_ID,
    EMPRESA_DEBENTURE_TABLE_ID,
    EMPRESA_PARTICIPACAO_ACIONARIA_TABLE_ID,
    FUNDO_PARTICIPACAO_TABLE_ID,
)
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection


@task(retries=3, retry_delay_seconds=60)
def get_source_last_modified(table_id: str) -> date:
    """Lê o dia da última substituição do CSV da tabela no CKAN.

    Args:
        table_id: Slug da tabela.

    Returns:
        O dia do `last_modified` do recurso.
    """
    return utils.get_source_last_modified(table_id=table_id)


@task(retries=3, retry_delay_seconds=60)
def download_table(table_id: str) -> Path:
    """Baixa o CSV da tabela e confere o MD5.

    Args:
        table_id: Slug da tabela.

    Returns:
        O caminho do CSV baixado.
    """
    return utils.download_table(table_id=table_id)


@task
def clean_table(table_id: str, csv_path: Path) -> Path:
    """Trata o CSV e particiona por ano.

    Args:
        table_id: Slug da tabela.
        csv_path: CSV baixado por `download_table`.

    Returns:
        O diretório particionado em formato Hive.
    """
    return utils.clean_table(table_id=table_id, csv_path=csv_path)


def get_latest_update(table_id: str) -> SourceInspection:
    """Etapa check_update: quando a fonte substituiu o arquivo pela última vez.

    Args:
        table_id: Slug da tabela.

    Returns:
        A data que o poll compara com o `Table.Update` da tabela.
    """
    return SourceInspection(
        reference_date=get_source_last_modified(table_id=table_id),
        compare_against="table_update",
    )


def extract_load_data(table_id: str, download_params: dict) -> ExtractAndLoad:
    """Etapa extract_and_load: baixa e trata o histórico inteiro.

    Args:
        table_id: Slug da tabela.
        download_params: O que o check_update mandou; não é usado.

    Returns:
        A pasta particionada e o modo de escrita.
    """
    csv_path = download_table(table_id=table_id)
    output_dir = clean_table(table_id=table_id, csv_path=csv_path)
    return ExtractAndLoad(data_path=str(output_dir), dump_mode="overwrite")


def empresa_participacao_acionaria_get_latest_update() -> SourceInspection:
    """Etapa check_update da empresa_participacao_acionaria."""
    return get_latest_update(table_id=EMPRESA_PARTICIPACAO_ACIONARIA_TABLE_ID)


def empresa_participacao_acionaria_extract_load_data(
    download_params: dict,
) -> ExtractAndLoad:
    """Etapa extract_and_load da empresa_participacao_acionaria."""
    return extract_load_data(
        table_id=EMPRESA_PARTICIPACAO_ACIONARIA_TABLE_ID,
        download_params=download_params,
    )


def fundo_participacao_get_latest_update() -> SourceInspection:
    """Etapa check_update da fundo_participacao."""
    return get_latest_update(table_id=FUNDO_PARTICIPACAO_TABLE_ID)


def fundo_participacao_extract_load_data(
    download_params: dict,
) -> ExtractAndLoad:
    """Etapa extract_and_load da fundo_participacao."""
    return extract_load_data(
        table_id=FUNDO_PARTICIPACAO_TABLE_ID, download_params=download_params
    )


def empresa_debenture_get_latest_update() -> SourceInspection:
    """Etapa check_update da empresa_debenture."""
    return get_latest_update(table_id=EMPRESA_DEBENTURE_TABLE_ID)


def empresa_debenture_extract_load_data(
    download_params: dict,
) -> ExtractAndLoad:
    """Etapa extract_and_load da empresa_debenture."""
    return extract_load_data(
        table_id=EMPRESA_DEBENTURE_TABLE_ID, download_params=download_params
    )


def desembolso_get_latest_update() -> SourceInspection:
    """Etapa check_update da desembolso."""
    return get_latest_update(table_id=DESEMBOLSO_TABLE_ID)


def desembolso_extract_load_data(download_params: dict) -> ExtractAndLoad:
    """Etapa extract_and_load da desembolso."""
    return extract_load_data(
        table_id=DESEMBOLSO_TABLE_ID, download_params=download_params
    )
