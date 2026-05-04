"""
Tasks for br_bcb_sicor
"""

from datetime import timedelta

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


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def search_sicor_links() -> pd.DataFrame:
    """
    Search for sicor download links and build a DataFrame.

    Returns:
        pd.DataFrame: DataFrame containing sicor download links and metadata.
    """
    links = get_sicor_download_links()
    df = build_sicor_download_df(links)
    return df


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def get_sicor_table_size(
    links_df: pd.DataFrame, table_id: str, download_all_files: bool = False
) -> int:
    """
    Get the total size (content_length) for a specific table_id.

    Args:
        links_df (pd.DataFrame): DataFrame containing sicor download links and metadata.
        table_id (str): The ID of the table.
        download_all_files (bool): If True, considers all files. Default is False.

    Returns:
        int: The total size in bytes.
    """
    table_df = filter_sicor_links(
        links_df, table_id, download_all_files=download_all_files
    )

    if table_df.empty:
        raise ValueError(
            f"No links found for table {table_id}. Check the sicor website to see if the name of the link has changed"
        )

    return int(table_df["content_length"].sum())


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download_table(
    search_sicor_links_result: pd.DataFrame,
    table_id: str,
    download_all_files: bool = False,
) -> str:
    """
    Create folder structure and download files for a specific table.

    Args:
        search_sicor_links_result (pd.DataFrame): DataFrame with download links.
        table_id (str): The ID of the table to download.
        download_all_files (bool): If True, downloads all files. Default is False.

    Returns:
        str: The path where files were downloaded.
    """
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

    return download_dir


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def create_load_dictionary() -> str:
    """
    Create the dictionary table and return its directory path.

    Returns:
        str: The path where the dictionary file was saved.
    """

    return create_dictionary()
