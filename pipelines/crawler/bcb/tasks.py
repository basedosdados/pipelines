"""
Tasks for br_bcb_sicor
"""

import pandas as pd
from prefect import task

from pipelines.crawler.bcb.utils import (
    build_sicor_download_df,
    create_dictionary,
    create_empreendimento,
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
    search_sicor_links_result: pd.DataFrame, table_id: str
) -> str:
    """
    Create folder structure and download files for a specific table.
    """
    download_dir = create_folder_structure(table_id)

    if table_id == "empreendimento":
        create_empreendimento(table_id, download_dir)

    else:
        download_standardize(search_sicor_links_result, table_id, download_dir)

    return download_dir


@task
def create_load_dictionary() -> str:
    """
    Create folder structure and download files for a specific table.
    """

    return create_dictionary()
