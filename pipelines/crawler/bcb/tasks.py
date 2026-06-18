"""
Tasks para br_bcb_sicor — Prefect 3.
"""

import pandas as pd
from prefect import task

from pipelines.constants import constants
from pipelines.crawler.bcb.utils import (
    build_sicor_download_df,
    create_dictionary,
    create_empreendimento,
    create_folder_structure,
    create_tables,
    filter_sicor_links,
    get_sicor_download_links,
)

TASK_RETRIES = constants.TASK_MAX_RETRIES.value
TASK_RETRY_DELAY_SECONDS = constants.TASK_RETRY_DELAY.value


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def search_sicor_links() -> pd.DataFrame:
    """Scrape o site do BCB e devolve DataFrame com links e metadados."""
    links = get_sicor_download_links()
    return build_sicor_download_df(links)


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def get_sicor_table_size(
    links_df: pd.DataFrame, table_id: str, download_all_files: bool = False
) -> int:
    """Soma o `content_length` dos arquivos da tabela `table_id`."""
    table_df = filter_sicor_links(
        links_df, table_id, download_all_files=download_all_files
    )

    if table_df.empty:
        raise ValueError(
            f"No links found for table {table_id}. Check the sicor website to see if the name of the link has changed"
        )

    return int(table_df["content_length"].sum())


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def download_table(
    search_sicor_links_result: pd.DataFrame,
    table_id: str,
    download_all_files: bool = False,
) -> str:
    """Baixa e transforma os arquivos da tabela `table_id`. Retorna o diretório."""
    download_dir = create_folder_structure(table_id)

    table_df = filter_sicor_links(
        search_sicor_links_result,
        table_id,
        download_all_files=download_all_files,
    )

    if table_id == "empreendimento":
        create_empreendimento(table_id, download_dir)
    else:
        create_tables(table_df, table_id, download_dir)

    return str(download_dir)


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def create_load_dictionary() -> str:
    """Gera o CSV do dicionário e retorna o diretório."""
    return create_dictionary()
