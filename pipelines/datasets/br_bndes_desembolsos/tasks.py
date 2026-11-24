"""
Tasks de br_bndes_desembolsos.

Cada task embrulha uma função de `utils.py`, onde fica a lógica.
"""

from pathlib import Path

from prefect import task

from pipelines.datasets.br_bndes_desembolsos import utils


@task(retries=3, retry_delay_seconds=30)
def get_source_last_modified(resource_id: str) -> str:
    """Lê a data da última republicação do recurso.

    Args:
        resource_id: Id do recurso no CKAN.

    Returns:
        A data do `last_modified`, no formato `%Y-%m-%d`.
    """
    return utils.get_source_last_modified(resource_id=resource_id)


@task(retries=3, retry_delay_seconds=60)
def download_table(resource_id: str, path: str, filename: str) -> Path:
    """Baixa o CSV e confere o MD5.

    Args:
        resource_id: Id do recurso no CKAN.
        path: Diretório de trabalho da tabela.
        filename: Nome com que o CSV é gravado em `input/`.

    Returns:
        O caminho do CSV baixado.
    """
    return utils.download_table(
        resource_id=resource_id, path=path, filename=filename
    )


@task
def clean_mensal(csv_path: Path) -> Path:
    """Limpa e particiona a mensal.

    Args:
        csv_path: CSV baixado por `download_table`.

    Returns:
        O diretório particionado, no formato esperado por `upload_to_gcs`.
    """
    return utils.clean_mensal(csv_path=csv_path)


@task
def clean_mapeamento_bndes_cnae(csv_path: Path) -> Path:
    """Limpa o mapeamento BNDES → CNAE.

    Args:
        csv_path: CSV baixado por `download_table`.

    Returns:
        O diretório com o parquet, no formato esperado por `upload_to_gcs`.
    """
    return utils.clean_mapeamento_bndes_cnae(csv_path=csv_path)
