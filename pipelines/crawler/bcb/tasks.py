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
    create_tables,
    get_sicor_download_links,
)


@task
def search_sicor_links() -> pd.DataFrame:
    """
    Search for sicor download links and build a DataFrame.

    Returns:
        pd.DataFrame: DataFrame containing sicor download links and metadata.
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

    Args:
        search_sicor_links_result (pd.DataFrame): DataFrame with download links.
        table_id (str): The ID of the table to download.

    Returns:
        str: The path where files were downloaded.
    """
    download_dir = create_folder_structure(table_id)

    if table_id == "empreendimento":
        create_empreendimento(table_id, download_dir)

    else:
        create_tables(search_sicor_links_result, table_id, download_dir)

    return download_dir


@task
def create_load_dictionary() -> str:
    """
    Create the dictionary table and return its directory path.

    Returns:
        str: The path where the dictionary file was saved.
    """

    return create_dictionary()
