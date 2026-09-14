"""
Tasks de br_ms_sinasc.

Cada task embrulha uma função de `utils.py`, onde fica a lógica.
"""

from pathlib import Path

from prefect import task

from pipelines.datasets.br_ms_sinasc import utils


@task(retries=3, retry_delay_seconds=30)
def get_source_max_year() -> str:
    """Lê na fonte até que ano ela publicou.

    Returns:
        O ano mais recente, no formato `%Y`.
    """
    return utils.get_source_max_year()


@task(retries=3, retry_delay_seconds=60)
def download_table(table_id: str, ano: int) -> Path:
    """Baixa os arquivos das UFs do ano.

    Args:
        table_id: Slug da tabela.
        ano: Ano a baixar.

    Returns:
        O diretório de entrada com os arquivos baixados.
    """
    return utils.download_table(table_id=table_id, ano=ano)


@task
def clean_table(table_id: str, ano: int) -> Path:
    """Limpa o ano já baixado e grava o particionado.

    Args:
        table_id: Slug da tabela.
        ano: Ano a limpar.

    Returns:
        O diretório particionado, no formato esperado por `upload_to_gcs`.
    """
    return utils.clean_table(table_id=table_id, ano=ano)
