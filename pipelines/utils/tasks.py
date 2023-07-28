# -*- coding: utf-8 -*-
"""
Helper tasks that could fit any pipeline.
"""
# pylint: disable=C0103, C0301, invalid-name, E1101, R0913

from datetime import timedelta, datetime
from pathlib import Path
from typing import Any, Union, List

import basedosdados as bd
import pandas as pd
import prefect
import ruamel.yaml as ryaml
from prefect import task
from prefect.backend import FlowRunView
from prefect.client import Client

from pipelines.constants import constants
from pipelines.utils.utils import (
    dump_header_to_csv,
    get_ids,
    parse_temporal_coverage,
    get_credentials_utils,
    create_update,
    extract_last_update,
    extract_last_date,
    get_first_date,
    log,
    get_credentials_from_secret,
    get_token,
)
from typing import Tuple


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
    wait=None,  # pylint: disable=unused-argument
) -> None:
    """
    Create table using BD+ and upload to GCS.
    """
    bd_version = bd.__version__
    log(f"USING BASEDOSDADOS {bd_version}")
    # pylint: disable=C0103
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
    table_staging = f"{tb.table_full_name['staging']}"
    # pylint: disable=C0103
    st = bd.Storage(dataset_id=dataset_id, table_id=table_id)
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
                f"MODE APPEND: Table ALREADY EXISTS:"
                f"\n{table_staging}"
                f"\n{storage_path_link}"
            )
        else:
            # the header is needed to create a table when dosen't exist
            log("MODE APPEND: Table DOSEN'T EXISTS\n" + "Start to CREATE HEADER file")
            header_path = dump_header_to_csv(data_path=data_path)
            log("MODE APPEND: Created HEADER file:\n" f"{header_path}")

            tb.create(
                path=header_path,
                if_storage_data_exists="replace",
                if_table_exists="replace",
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
        log("MODE OVERWRITE: Table DOSEN'T EXISTS\n" + "Start to CREATE HEADER file")
        header_path = dump_header_to_csv(data_path=data_path)
        log("MODE OVERWRITE: Created HEADER file:\n" f"{header_path}")

        tb.create(
            path=header_path,
            if_storage_data_exists="replace",
            if_table_exists="replace",
        )

        log(
            "MODE OVERWRITE: Sucessfully CREATED TABLE\n"
            f"{table_staging}\n"
            f"{storage_path_link}"
        )

        st.delete_table(mode="staging", bucket_name=st.bucket_name, not_found_ok=True)
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
        tb.append(filepath=data_path, if_exists="replace")

        log(
            f"STEP UPLOAD: Successfully uploaded {data_path} to Storage:\n"
            f"{storage_path}\n"
            f"{storage_path_link}"
        )
    else:
        # pylint: disable=C0301
        log("STEP UPLOAD: Table does not exist in STAGING, need to create first")


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
# pylint: disable=R0914
def update_metadata(dataset_id: str, table_id: str, fields_to_update: list) -> None:
    """
    Update metadata for a selected table

    dataset_id: dataset_id,
    table_id: table_id,
    fields_to_update: list of dictionaries with key and values to be updated
    """
    # add credentials to config.toml
    # TODO: remove this because bd 2.0 does not have Metadata class
    handle = bd.Metadata(dataset_id=dataset_id, table_id=table_id)
    handle.create(if_exists="replace")

    yaml = ryaml.YAML()
    yaml.preserve_quotes = True
    yaml.indent(mapping=4, sequence=6, offset=4)

    config_file = handle.filepath.as_posix()  # noqa

    with open(config_file, encoding="utf-8") as fp:
        data = yaml.load(fp)

    # this is, of course, very slow but very few fields will be update each time, so the cubic algo will not have major performance consequences
    for field in fields_to_update:
        for k, v in field.items():
            if isinstance(v, dict):
                for i, j in v.items():
                    data[k][i] = j
            else:
                data[k] = v

    with open(config_file, "w", encoding="utf-8") as fp:
        yaml.dump(data, fp)

    if handle.validate():
        handle.publish(if_exists="replace")
        log(f"Metadata for {table_id} updated")
    else:
        log("Fail to validate metadata.")


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
        start_date = (
            f"{dates[0].year}-{dates[0].strftime('%m')}-{dates[0].strftime('%d')}"
        )
        end_date = (
            f"{dates[-1].year}-{dates[-1].strftime('%m')}-{dates[-1].strftime('%d')}"
        )
        return start_date + "(" + interval + ")" + end_date
    if time_unit == "month":
        start_date = f"{dates[0].year}-{dates[0].strftime('%m')}"
        end_date = f"{dates[-1].year}-{dates[-1].strftime('%m')}"
        return start_date + "(" + interval + ")" + end_date
    if time_unit == "year":
        start_date = f"{dates[0].year}"
        end_date = f"{dates[-1].year}"
        return start_date + "(" + interval + ")" + end_date

    raise ValueError("time_unit must be one of the following: day, month, year")


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
    return client.set_flow_run_name(flow_run_id, f"{prefix}{dataset_id}.{table_id}")


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


########################
#
# Update Django Metadata
#
########################
@task  # noqa
def update_django_metadata(
    dataset_id: str,
    table_id: str,
    metadata_type: str,
    _last_date=None,
    date_format: str = "yy-mm-dd",
    bq_last_update: bool = True,
    bq_table_last_year_month: bool = False,
    api_mode: str = "prod",
    billing_project_id: str = "basedosdados-dev",
):
    """
    Updates Django metadata.

    Args:
        -   `dataset_id (str):` The ID of the dataset.
        -   `table_id (str):` The ID of the table.
        -   `metadata_type (str):` The type of metadata to update.
        -   `_last_date (optional):` The last date for metadata update if `bq_last_update` is False. Defaults to None.
        -   `date_format (str, optional):` The date format to use when parsing dates ('yy-mm-dd', 'yy-mm', or 'yy'). Defaults to 'yy-mm-dd'.
        -   `bq_last_update (bool, optional):` Flag indicating whether to use BigQuery's last update date for metadata.
            If True, `_last_date` is ignored. Defaults to True.
        -   `api_mode (str, optional):` The API mode to be used ('prod', 'staging'). Defaults to 'prod'.
        -   `billing_project_id (str):` the billing_project_id to be used when the extract_last_update function is triggered. Note that it has
        to be equal to the prefect agent. For prod agents use basedosdados where as for dev agents use basedosdados-dev. The default value is
        to 'basedosdados-dev'.
        -   `bq_table_last_year_month (bool):` if true extract YYYY-MM from the table in Big Query to update the Coverage. Note
        that in needs the table to have ano and mes columns.

    Example:

        Eg 1. In this example the function will look for example_table in basedosdados project.
        It will look for a column name `data` in date_format = 'yyyy-mm-dd' in the BQ and retrieve its max value.
        ```
        update_django_metadata(
                dataset_id = 'example_dataset',
                table_id = 'example_table,
                metadata_type="DateTimeRange",
                bq_last_update=False,
                bq_table_last_year_month=True,
                billing_project_id="basedosdados",
                api_mode="prod",
                date_format="yy-mm-dd",
                upstream_tasks=[wait_for_materialization],
            )
        ```
    Returns:
        -   None

    Raises:
        -   Exception: If the metadata_type is not supported.

    """
    (email, password) = get_credentials_utils(secret_path=f"api_user_{api_mode}")

    ids = get_ids(
        dataset_id,
        table_id,
        email,
        password,
    )
    log(f"IDS:{ids}")

    if metadata_type == "DateTimeRange":
        if bq_last_update:
            log(
                f"Attention! bq_last_update was set to TRUE, it will update the temporal coverage according to the metadata of the last modification made to {table_id}.{dataset_id}"
            )
            last_date = extract_last_update(
                dataset_id,
                table_id,
                date_format,
                billing_project_id=billing_project_id,
            )

            resource_to_temporal_coverage = parse_temporal_coverage(f"{last_date}")
            resource_to_temporal_coverage["coverage"] = ids.get("coverage_id")
            log(f"Mutation parameters: {resource_to_temporal_coverage}")

            create_update(
                query_class="allDatetimerange",
                query_parameters={"$coverage_Id: ID": ids.get("coverage_id")},
                mutation_class="CreateUpdateDateTimeRange",
                mutation_parameters=resource_to_temporal_coverage,
                update=True,
                email=email,
                password=password,
                api_mode=api_mode,
            )
        elif bq_table_last_year_month:
            log(
                f"Attention! bq_table_last_year_month was set to TRUE, this function will update the temporal coverage according to the most recent date in the data or ano-mes columns of {table_id}.{dataset_id}"
            )
            last_date = extract_last_date(
                dataset_id,
                table_id,
                date_format=date_format,
                billing_project_id=billing_project_id,
            )

            resource_to_temporal_coverage = parse_temporal_coverage(f"{last_date}")
            resource_to_temporal_coverage["coverage"] = ids.get("coverage_id")
            log(f"Mutation parameters: {resource_to_temporal_coverage}")

            create_update(
                query_class="allDatetimerange",
                query_parameters={"$coverage_Id: ID": ids.get("coverage_id")},
                mutation_class="CreateUpdateDateTimeRange",
                mutation_parameters=resource_to_temporal_coverage,
                update=True,
                email=email,
                password=password,
                api_mode=api_mode,
            )
        else:
            last_date = _last_date
            log(f"Última data {last_date}")

            resource_to_temporal_coverage = parse_temporal_coverage(f"{last_date}")

            resource_to_temporal_coverage["coverage"] = ids.get("coverage_id")
            log(f"Mutation parameters: {resource_to_temporal_coverage}")

            create_update(
                query_class="allDatetimerange",
                query_parameters={"$coverage_Id: ID": ids.get("coverage_id")},
                mutation_class="CreateUpdateDateTimeRange",
                mutation_parameters=resource_to_temporal_coverage,
                update=True,
                email=email,
                password=password,
                api_mode=api_mode,
            )
