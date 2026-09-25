"""
Tasks de br_inep_enem.

Cada task embrulha uma função de `utils.py`, onde fica a lógica.
"""

from pathlib import Path

from prefect import task

from pipelines.datasets.br_inep_enem import utils


@task(retries=3, retry_delay_seconds=30)
def get_source_max_date() -> str:
    """Lê na página do INEP o ano mais recente publicado.

    Returns:
        O ano mais recente, no formato `%Y`.
    """
    return utils.get_source_max_date()


@task(retries=3, retry_delay_seconds=60)
def download_table(table_id: str, ano: str) -> Path:
    """Baixa a edição e extrai o CSV da tabela.

    Args:
        table_id: Slug da tabela.
        ano: Edição a baixar, no formato `%Y`.

    Returns:
        O diretório de entrada com o CSV extraído.
    """
    return utils.download_table(table_id=table_id, ano=ano)


@task
def clean_table(table_id: str, ano: str) -> Path:
    """Limpa e particiona a edição da tabela.

    Args:
        table_id: Slug da tabela.
        ano: Edição a limpar, no formato `%Y`.

    Returns:
        O diretório particionado, no formato esperado por `upload_to_gcs`.
    """
    return utils.clean_table(table_id=table_id, ano=ano)


@task
def resolve_years(
    backfill_years: list[str] | None, source_max_date: str
) -> list[str]:
    """Decide quais edições a execução vai carregar.

    Args:
        backfill_years: Edições a recarregar, no formato `%Y`, ou None.
        source_max_date: Edição mais recente publicada, no formato `%Y`.

    Returns:
        As edições a carregar, em ordem crescente.
    """
    return utils.resolve_years(
        backfill_years=backfill_years, source_max_date=source_max_date
    )
