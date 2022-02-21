"""
General utilities for all pipelines.
"""
from pathlib import Path
from typing import Any, Dict, Union
import logging

import basedosdados as bd
# from mysqlx import Row
import pandas as pd

import prefect
from prefect import task


def log(msg: Any, level: str = "info") -> None:
    """
    Logs a message to prefect's logger.
    """
    levels = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }
    if level not in levels:
        raise ValueError(f"Invalid log level: {level}")
    prefect.context.logger.log(levels[level], msg)  # pylint: disable=E1101


@prefect.task(checkpoint=False)
def log_task(msg: Any, level: str = "info"):
    """
    Logs a message to prefect's logger.
    """
    log(msg, level)


def run_local(flow: prefect.Flow, parameters: Dict[str, Any] = None):
    """
    Runs a flow locally.
    """
    # Setup for local run
    flow.storage = None
    flow.run_config = None
    flow.schedule = None

    # Run flow
    if parameters:
        return flow.run(parameters=parameters)
    return flow.run()


###############
#
# Upload to GCS
#
###############
@task
def create_header(path: Union[str, Path]):
    """
    Writes a header to a CSV file.
    """
    # Remove filename from path
    path = Path(path)
    dataframe = pd.read_csv(path, nrows=10)

    # Create directory if it doesn't exist
    save_header_path = Path("data/header.csv")
    save_header_path.parent.mkdir(parents=True, exist_ok=True)

    # Write dataframe to CSV
    dataframe.to_csv(save_header_path, index=False, encoding="utf-8")
    log(f"Wrote header CSV: {path}")

    return save_header_path


@task
def upload_to_gcs(
    path: Union[str, Path],
    dataset_id: str,
    table_id: str,
    wait=None,  # pylint: disable=unused-argument
) -> None:
    """
    Uploads a bunch of CSVs using BD+
    """
    # pylint: disable=C0103
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
    # st = bd.Storage(dataset_id=dataset_id, table_id=table_id)

    if tb.table_exists(mode="staging"):
        # the name of the files need to be the same or the data doesn't get overwritten
        tb.append(
            filepath=path,
            if_exists="replace",
        )

        log(
            f"Successfully uploaded {path} to {tb.bucket_name}.staging.{dataset_id}.{table_id}"
        )

    else:
        # pylint: disable=C0301
        log(
            "Table does not exist in STAGING, need to create it in local first.\nCreate and publish the table in BigQuery first."
        )


@task
def create_bd_table(
    path: Union[str, Path],
    dataset_id: str,
    table_id: str,
    dump_type: str,
    wait=None,  # pylint: disable=unused-argument
) -> None:
    """
    Create table using BD+
    """
    # pylint: disable=C0103
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)

    # pylint: disable=C0103
    st = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    
    # full dump
    if dump_type == "append":
        if tb.table_exists(mode="staging"):
            log(
                f"Mode append: Table {st.bucket_name}.{dataset_id}.{table_id} already exists"
            )
        else:
            tb.create(
                path=path,
                location="southamerica-east1",
            )
            log(
                f"Mode append: Sucessfully created a new table {st.bucket_name}.{dataset_id}.{table_id}"
            )  # pylint: disable=C0301

            st.delete_table(
                mode="staging", bucket_name=st.bucket_name, not_found_ok=True
            )
            log(
                f"Mode append: Sucessfully remove header data from {st.bucket_name}.{dataset_id}.{table_id}"
            )  # pylint: disable=C0301
    elif dump_type == "overwrite":
        if tb.table_exists(mode="staging"):
            log(
                f"Mode overwrite: Table {st.bucket_name}.{dataset_id}.{table_id} already exists, DELETING OLD DATA!"
            )  # pylint: disable=C0301
            st.delete_table(
                mode="staging", bucket_name=st.bucket_name, not_found_ok=True
            )
        ## prod datasets is public if the project is datario. staging are private im both projects
        tb.create(
            path=path,
            if_storage_data_exists="replace",
            if_table_config_exists="replace",
            if_table_exists="replace"
            # location="southamerica-east1",
        )

        log(
            f"Mode overwrite: Sucessfully created table {st.bucket_name}.{dataset_id}.{table_id}"
        )
        st.delete_table(mode="staging", bucket_name=st.bucket_name, not_found_ok=True)
        log(
            f"Mode overwrite: Sucessfully remove header data from {st.bucket_name}.{dataset_id}.{table_id}"
        )  # pylint: disable=C0301
