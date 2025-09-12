# -*- coding: utf-8 -*-
"""
Helper tasks that could fit any pipeline.
"""
# pylint: disable=C0103, C0301, invalid-name, E1101, R0913

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, List, Union

import basedosdados as bd
import pandas as pd
import prefect
from prefect import task
from prefect.backend import FlowRunView
from prefect.client import Client
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.utils import (
    dump_header_to_csv,
    get_credentials_from_secret,
    log,
)


@task
def log_task(msg: Any, level: str = "info"):
    """
    A task that logs a message.
    """
    log(msg=msg, level=level)


##################
#
# Hashicorp Vault
#
##################


@task(checkpoint=False, nout=2)
def get_credentials(secret_path: str):
    """
    Returns the user and password for the given secret path.
    """
    log(f"Getting user and password for secret path: {secret_path}")
    return get_credentials_from_secret(secret_path)


def upload_to_gcs(
    data_path: Union[str, Path],
    dataset_id: str,
    table_id: str,
    dump_mode: str,
    bucket_name: str = "basedosdados",
    source_format: str = "csv",
) -> None:
    """
    Create table using BD+ and upload to GCS.
    """
    bd_version = bd.__version__
    log(f"USING BASEDOSDADOS {bd_version}")
    # pylint: disable=C0103
    tb = bd.Table(
        dataset_id=dataset_id, table_id=table_id, bucket_name=bucket_name
    )
    table_staging = f"{tb.table_full_name['staging']}"
    # pylint: disable=C0103
    st = bd.Storage(
        dataset_id=dataset_id, table_id=table_id, bucket_name=bucket_name
    )
    storage_path = f"{st.bucket_name}.staging.{dataset_id}.{table_id}"
    storage_path_link = f"https://console.cloud.google.com/storage/browser/{st.bucket_name}/staging/{dataset_id}/{table_id}"

    #####################################
    #
    # MANAGEMENT OF TABLE CREATION
    #
    #####################################
    log("STARTING TABLE CREATION MANAGEMENT")
    if dump_mode == "append":
        if tb.table_exists(mode="staging"):
            log(
                f"MODE APPEND: Table ALREADY EXISTS:\n{table_staging}\n{storage_path_link}"
            )
        else:
            # the header is needed to create a table when dosen't exist
            log(
                "MODE APPEND: Table DOSEN'T EXISTS\n"
                + "Start to CREATE HEADER file"
            )
            header_path = dump_header_to_csv(
                data_path=data_path, source_format=source_format
            )
            log(f"MODE APPEND: Created HEADER file:\n{header_path}")

            tb.create(
                path=header_path,
                if_storage_data_exists="replace",
                if_table_exists="replace",
                source_format=source_format,
            )

            log(
                "MODE APPEND: Sucessfully CREATED A NEW TABLE:\n"
                f"{table_staging}\n"
                f"{storage_path_link}"
            )  # pylint: disable=C0301

            st.delete_table(
                mode="staging", bucket_name=st.bucket_name, not_found_ok=True
            )
            log(
                "MODE APPEND: Sucessfully REMOVED HEADER DATA from Storage:\n"
                f"{storage_path}\n"
                f"{storage_path_link}"
            )  # pylint: disable=C0301
    elif dump_mode == "overwrite":
        if tb.table_exists(mode="staging"):
            log(
                "MODE OVERWRITE: Table ALREADY EXISTS, DELETING OLD DATA!\n"
                f"{storage_path}\n"
                f"{storage_path_link}"
            )  # pylint: disable=C0301
            st.delete_table(
                mode="staging", bucket_name=st.bucket_name, not_found_ok=True
            )
            log(
                "MODE OVERWRITE: Sucessfully DELETED OLD DATA from Storage:\n"
                f"{storage_path}\n"
                f"{storage_path_link}"
            )  # pylint: disable=C0301
            tb.delete(mode="all")
            log(
                "MODE OVERWRITE: Sucessfully DELETED TABLE:\n"
                f"{table_staging}\n"
                f"{tb.table_full_name['prod']}"
            )  # pylint: disable=C0301

        # the header is needed to create a table when dosen't exist
        # in overwrite mode the header is always created
        log(
            "MODE OVERWRITE: Table DOSEN'T EXISTS\n"
            + "Start to CREATE HEADER file"
        )
        header_path = dump_header_to_csv(
            data_path=data_path, source_format=source_format
        )
        log(f"MODE OVERWRITE: Created HEADER file:\n{header_path}")

        tb.create(
            path=header_path,
            if_storage_data_exists="replace",
            if_table_exists="replace",
            source_format=source_format,
        )

        log(
            f"MODE OVERWRITE: Sucessfully CREATED TABLE\n{table_staging}\n{storage_path_link}"
        )

        st.delete_table(
            mode="staging", bucket_name=st.bucket_name, not_found_ok=True
        )
        log(
            f"MODE OVERWRITE: Sucessfully REMOVED HEADER DATA from Storage\n:"
            f"{storage_path}\n"
            f"{storage_path_link}"
        )  # pylint: disable=C0301

    #####################################
    #
    # Uploads a bunch of CSVs using BD+
    #
    #####################################

    log("STARTING UPLOAD TO GCS")
    if tb.table_exists(mode="staging"):
        # the name of the files need to be the same or the data doesn't get overwritten
        st.upload(
            path=data_path,
            mode="staging",
            if_exists="replace",
        )

        log(
            f"STEP UPLOAD: Successfully uploaded {data_path} to Storage:\n"
            f"{storage_path}\n"
            f"{storage_path_link}"
        )
    else:
        # pylint: disable=C0301
        log(
            "STEP UPLOAD: Table does not exist in STAGING, need to create first"
        )


def is_prod_agent() -> bool:
    flow_run_id = prefect.context.get("flow_run_id")

    if flow_run_id is None:
        return False

    labels = FlowRunView.from_flow_run_id(flow_run_id).labels

    if len(labels) == 0:
        raise Exception(f"Dont found label for flow: {flow_run_id}")
    if len(labels) > 1:
        raise Exception(
            f"Found more than one label {labels=}, flow {flow_run_id}"
        )

    for label in labels:
        if label == "basedosdados":
            return True

    return False


@task
def dbt_materialize(
    dataset_id: str,
    table_id: str,
    prod_agent: bool,
    dbt_command: str,
    dbt_target: str,
    dbt_alias: bool,
    download_csv_file: bool,
) -> None:
    run_name = f"Materialize {dataset_id}.{table_id}"

    materialization_flow = create_flow_run.run(
        flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,  # type: ignore
        project_name=constants.PREFECT_DEFAULT_PROJECT.value  # type: ignore
        if prod_agent
        else constants.PREFECT_STAGING_PROJECT.value,
        parameters={  # type: ignore
            "dataset_id": dataset_id,
            "table_id": table_id,
            "target": dbt_target,
            "dbt_command": dbt_command,
            "dbt_alias": dbt_alias,
            "disable_elementary": False,
            "download_csv_file": download_csv_file,
        },
        labels=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value  # type: ignore
        if prod_agent
        else constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
        run_name=run_name,  # type: ignore
    )

    wait_for_flow_run.run(
        materialization_flow,  # type: ignore
        stream_states=True,  # type: ignore
        stream_logs=True,  # type: ignore
        raise_final_state=True,  # type: ignore
    )


###############
#
# Upload to GCS
#
###############
@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def create_table_and_upload_to_gcs(
    data_path: Union[str, Path],
    dataset_id: str,
    table_id: str,
    dump_mode: str,
    dbt_alias: bool = True,
    source_format: str = "csv",
    wait=None,
) -> None:
    """
    Create table using BD+ and upload to GCS.
    """
    prod_agent = is_prod_agent()

    # Upload data to basedosdados-dev bucket
    upload_to_gcs(
        data_path=data_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        source_format=source_format,
        bucket_name="basedosdados-dev",
    )

    dbt_materialize.run(
        dataset_id=dataset_id,
        table_id=table_id,
        prod_agent=prod_agent,
        dbt_command="run/test",
        dbt_target="dev",
        dbt_alias=dbt_alias,
        download_csv_file=False,
    )


@task
def create_table_and_upload_to_gcs_prod(
    data_path: Union[str, Path],
    dataset_id: str,
    table_id: str,
    dump_mode: str,
    target: str = "prod",
    dbt_alias: bool = True,
    source_format: str = "csv",
    wait=None,
) -> None:
    prod_agent = is_prod_agent()
    if prod_agent:
        log("Running in prod agent. Uploading data to basedosdados-staging")
        upload_to_gcs(
            data_path=data_path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode=dump_mode,
            source_format=source_format,
        )

        dbt_materialize.run(
            dataset_id=dataset_id,
            table_id=table_id,
            prod_agent=prod_agent,
            dbt_command="run",
            dbt_target="prod",
            dbt_alias=dbt_alias,
            download_csv_file=True,
        )
    else:
        log(
            "Create table and upload to gcs prod skip because running outside prod agent",
            level="warn",
        )


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def get_temporal_coverage(
    filepath: str, date_cols: list, time_unit: str, interval: str
) -> str:
    """
    Generates a temporal coverage string from a csv.
    The pattern follows the BD's Style Manual (https://basedosdados.github.io/mais/style_data/#cobertura-temporal)

    args:
    filepath: csv filepath
    date_cols: date columns to use as reference (use the order [year, month, day])
    time_unit: day | month | year
    interval: time between dates.
    For example, if the time_unit is month and the data is update every quarter, then the intervel is 3.
    """
    if len(date_cols) == 1:
        date_col = date_cols[0]
        df = pd.read_csv(filepath, usecols=[date_col], parse_dates=[date_col])
        dates = df[date_col].to_list()
        # keep only valid dates
        dates = [d for d in dates if isinstance(d, pd.Timestamp)]
        if len(dates) == 0:
            raise ValueError("Selected date col has no valid date")
        dates.sort()
    elif len(date_cols) == 2:
        year = date_cols[0]
        month = date_cols[1]
        df = pd.read_csv(filepath, usecols=[year, month])
        df["date"] = [
            datetime.strptime(str(x) + "-" + str(y) + "-" + "1", "%Y-%m-%d")
            for x, y in zip(df[year], df[month])
        ]
        dates = df["date"].to_list()
        # keep only valid dates
        dates = [d for d in dates if isinstance(d, pd.Timestamp)]
        if len(dates) == 0:
            raise ValueError("Selected date col has no valid date")
        dates.sort()
    elif len(date_cols) == 3:
        year = date_cols[0]
        month = date_cols[1]
        day = date_cols[2]
        df = pd.read_csv(filepath, usecols=[year, month])
        df["date"] = [
            datetime.strptime(str(x) + "-" + str(y) + "-" + str(y), "%Y-%m-%d")
            for x, y in zip(df[year], df[month], df[day])
        ]
        dates = df["date"].to_list()
        # keep only valid dates
        dates = [d for d in dates if isinstance(d, pd.Timestamp)]
        if len(dates) == 0:
            raise ValueError("Selected date col has no valid date")
        dates.sort()
    else:
        raise ValueError(
            "date_cols must be a list with up to 3 elements in the following order [year, month, day]"
        )

    if time_unit == "day":
        start_date = f"{dates[0].year}-{dates[0].strftime('%m')}-{dates[0].strftime('%d')}"
        end_date = f"{dates[-1].year}-{dates[-1].strftime('%m')}-{dates[-1].strftime('%d')}"
        return start_date + "(" + interval + ")" + end_date
    if time_unit == "month":
        start_date = f"{dates[0].year}-{dates[0].strftime('%m')}"
        end_date = f"{dates[-1].year}-{dates[-1].strftime('%m')}"
        return start_date + "(" + interval + ")" + end_date
    if time_unit == "year":
        start_date = f"{dates[0].year}"
        end_date = f"{dates[-1].year}"
        return start_date + "(" + interval + ")" + end_date

    raise ValueError(
        "time_unit must be one of the following: day, month, year"
    )


# pylint: disable=W0613
@task  # noqa
def rename_current_flow_run(msg: str, wait=None) -> bool:
    """
    Rename the current flow run.
    """
    flow_run_id = prefect.context.get("flow_run_id")
    client = Client()
    return client.set_flow_run_name(flow_run_id, msg)


@task  # noqa
def rename_current_flow_run_dataset_table(
    prefix: str, dataset_id, table_id, wait=None
) -> bool:
    """
    Rename the current flow run.
    """
    flow_run_id = prefect.context.get("flow_run_id")
    client = Client()
    return client.set_flow_run_name(
        flow_run_id, f"{prefix}{dataset_id}.{table_id}"
    )


@task  # noqa
def get_current_flow_labels() -> List[str]:
    """
    Get the labels of the current flow.
    """
    flow_run_id = prefect.context.get("flow_run_id")
    flow_run_view = FlowRunView.from_flow_run_id(flow_run_id)
    return flow_run_view.labels


@task  # noqa
def get_date_time_str(wait=None) -> str:
    """
    Get current time as string
    """
    return datetime.now().strftime("%Y-%m-%d %HH:%MM")
