"""
Tasks da tabela operacoes_pre_embarque.

Cada task embrulha uma função de `utils.py`, onde fica a lógica.
"""

from pathlib import Path

from prefect import task

from pipelines.datasets.br_bndes_operacoes_contratadas import utils


@task(retries=3, retry_delay_seconds=30)
def get_source_last_modified() -> str:
    """Lê a data da última republicação do recurso.

    Returns:
        A data do `last_modified`, no formato `%Y-%m-%d`.
    """
    return utils.get_source_last_modified()


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
