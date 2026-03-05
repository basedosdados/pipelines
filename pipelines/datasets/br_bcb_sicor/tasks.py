"""
Tasks for br_bcb_sicor
"""

import pandas as pd
from prefect import task

from pipelines.datasets.br_bcb_sicor.utils import (
    build_sicor_download_df,
    create_folder_structure,
    download_standardize,
    get_sicor_download_links,
)


@task
def search_sicor_links() -> pd.DataFrame:
    """
    Search for sicor download links and build a DataFrame.
    """
    links = get_sicor_download_links()
    df = build_sicor_download_df(links)
    return df


@task
def download_table(
    search_sicor_links_result: pd.DataFrame, id_tabela: str
) -> str:
    """
    Create folder structure and download files for a specific table.
    """
    download_dir = create_folder_structure(id_tabela)
    download_standardize(search_sicor_links_result, id_tabela, download_dir)
    return download_dir
