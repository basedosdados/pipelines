"""
Tasks de br_bd_diretorios_brasil.

Cada task embrulha uma função de `utils.py`, onde fica a lógica.
"""

from pathlib import Path

import pandas as pd
from prefect import task

from pipelines.datasets.br_bd_diretorios_brasil import utils
from pipelines.datasets.br_bd_diretorios_brasil.constants import constants


@task
def get_source_max_date() -> str:
    """Devolve a data que o Catálogo extraído representa.

    Returns:
        A data de hoje, no formato `%Y-%m-%d`.
    """
    return utils.get_source_max_date()


@task(retries=3, retry_delay_seconds=300)
def download_catalogo() -> Path:
    """Baixa o CSV do Catálogo de Escolas do Inep.

    Returns:
        O caminho do CSV baixado.
    """
    return utils.download_catalogo(
        input_dir=Path(constants.PATH.value) / "input"
    )


@task(retries=3, retry_delay_seconds=30)
def build_municipio_lookup() -> dict[tuple[str, str], str]:
    """Lê o diretório de municípios para resolver o `id_municipio`.

    Returns:
        O mapa de (nome normalizado, UF) para o código do IBGE.
    """
    return utils.build_municipio_lookup_from_bq()


@task(retries=3, retry_delay_seconds=30)
def fetch_diretorio_publicado() -> pd.DataFrame:
    """Lê o diretório de escolas publicado em produção.

    Returns:
        Uma linha por `id_escola` da tabela publicada.
    """
    return utils.fetch_diretorio_publicado()


@task(retries=3, retry_delay_seconds=30)
def fetch_censo_escolar() -> pd.DataFrame:
    """Lê do Censo Escolar uma linha por escola, com município e UF.

    Returns:
        Uma linha por `id_escola` do Censo Escolar, com o município e a UF do
        último ano em que a escola aparece.
    """
    return utils.fetch_censo_escolar()


@task
def clean_catalogo(
    csv_path: Path,
    municipio_lookup: dict[tuple[str, str], str],
    diretorio_publicado: pd.DataFrame,
    censo_escolar: pd.DataFrame,
) -> Path:
    """Limpa o Catálogo e o une ao diretório publicado e ao Censo Escolar.

    Args:
        csv_path: CSV baixado por `download_catalogo`.
        municipio_lookup: Mapa devolvido por `build_municipio_lookup`.
        diretorio_publicado: Tabela devolvida por `fetch_diretorio_publicado`.
        censo_escolar: Tabela devolvida por `fetch_censo_escolar`.

    Returns:
        O caminho do parquet que sobe para a staging.
    """
    return utils.clean_catalogo(
        csv_path,
        Path(constants.PATH.value) / "output",
        municipio_lookup=municipio_lookup,
        diretorio_publicado=diretorio_publicado,
        censo_escolar=censo_escolar,
    )
