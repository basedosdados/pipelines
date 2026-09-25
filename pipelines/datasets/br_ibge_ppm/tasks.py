"""
Tasks de br_ibge_ppm.

Cada task embrulha uma função de `utils.py`, onde fica a lógica.
"""

from pathlib import Path

from prefect import task

from pipelines.datasets.br_ibge_ppm import utils


@task(retries=3, retry_delay_seconds=30)
def get_source_max_date(table_id: str) -> str:
    """Lê nos metadados do SIDRA até que ano a fonte publicou a tabela.

    Args:
        table_id: Slug da tabela.

    Returns:
        O ano mais recente publicado, no formato `%Y`.
    """
    return utils.get_source_max_date(table_id=table_id)


@task
def resolve_years(
    table_id: str, backfill_years: list[str] | None, source_max_date: str
) -> list[str]:
    """Decide quais anos a execução vai carregar.

    Args:
        table_id: Slug da tabela.
        backfill_years: Anos a recarregar, no formato `%Y`, ou None.
        source_max_date: Ano mais recente publicado, no formato `%Y`.

    Returns:
        Os anos a carregar, em ordem crescente.
    """
    return utils.resolve_years(
        table_id=table_id,
        backfill_years=backfill_years,
        source_max_date=source_max_date,
    )


@task(retries=3, retry_delay_seconds=60)
def download_table(table_id: str, ano: str) -> Path:
    """Baixa da API do IBGE as séries que compõem a tabela no ano.

    Args:
        table_id: Slug da tabela.
        ano: Ano a baixar, no formato `%Y`.

    Returns:
        O diretório de entrada com os JSONs baixados.
    """
    return utils.download_table(table_id=table_id, ano=ano)


@task
def clean_table(table_id: str, ano: str) -> Path:
    """Junta as séries do ano numa tabela e grava o particionado.

    Args:
        table_id: Slug da tabela.
        ano: Ano a limpar, no formato `%Y`.

    Returns:
        O diretório particionado, no formato esperado por `upload_to_gcs`.
    """
    return utils.clean_table(table_id=table_id, ano=ano)
