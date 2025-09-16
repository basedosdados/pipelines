"""
Tasks for transfer_files_to_prod
"""

import os
import pathlib

from google.cloud import storage
from prefect import task


@task
def download_files_from_bucket_folders(
    dataset_id: str,
    table_id: str,
    folders: str | list[str],
    extension: str | None = None,
):
    """
    Downloads files with a specified extension from specified folders within a Google Cloud Storage bucket
    and saves them to a local directory.

    Parameters:
        dataset_id (str): The ID of the dataset.
        table_id (str): The ID of the table.
        folders (Union[str, List[str]]): Either a single folder name or a list of folder names within the bucket
            containing the files.
        extension (str, optional): The file extension to filter. If None, downloads all files. Default is None.

    Returns:
        str: The base path where the files are saved.
    """
    os.makedirs("/tmp/data/backup/", exist_ok=True)
    storage_client = storage.Client()

    if isinstance(folders, str):
        folders = [folders]

    for folder in folders:
        # List blobs (files) within the specified folder in the bucket
        blobs_in_bucket = storage_client.list_blobs(
            "basedosdados-dev",
            prefix=f"staging/{dataset_id}/{table_id}/{folder}/",
        )
        blob_list = list(blobs_in_bucket)

        for blob in blob_list:
            if extension is None or blob.name.endswith(extension):
                savepath = "/tmp/data/backup/"
                filename = blob.name.split("/")[-1]
                blob_folder = blob.name.replace(filename, "")

                (pathlib.Path(savepath) / blob_folder).mkdir(
                    parents=True, exist_ok=True
                )

                savepath = f"{savepath}/{blob.name}"
                blob.download_to_filename(filename=savepath)

    return f"/tmp/data/backup/staging/{dataset_id}/{table_id}/"
