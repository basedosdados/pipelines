# -*- coding: utf-8 -*-
"""
Helper tasks that could fit any pipeline.
"""
# pylint: disable=C0103, C0301, invalid-name, E1101, R0913

import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from time import sleep
from typing import Any, List, Union

import basedosdados as bd
import jinja2
import pandas as pd
import prefect
from basedosdados.core.base import Base
from basedosdados.download.download import _google_client
from dbt.cli.main import dbtRunner
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud.bigquery import TableReference
from prefect import task
from prefect.backend import FlowRunView
from prefect.client import Client

from pipelines.constants import constants
from pipelines.utils.metadata.utils import get_url
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
    source_format: str = "csv",
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
        tb.append(filepath=data_path, if_exists="replace")

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


def _process_dbt_log_file(log_path: str) -> pd.DataFrame:
    """
    Process the contents of a dbt log file and return a DataFrame containing the parsed log entries.
    This is the primary function for extracting log information from dbt log files.

    Args:
        log_path (str): The path to the dbt log file.

    Returns:
        pd.DataFrame: A DataFrame containing the parsed log entries with columns for time, level, and text.
    """

    if not os.path.exists(log_path):
        log(f"Log file not found at path: {log_path}", level="warning")
        return pd.DataFrame(columns=["time", "level", "text"])

    try:
        with open(
            log_path, "r", encoding="utf-8", errors="ignore"
        ) as log_file:
            log_content = log_file.read()

        log(f"Log file size: {len(log_content)} bytes", level="debug")

        # Try different patterns to parse the logs
        # Pattern 1: Look for timestamp followed by log level in brackets
        entries = re.findall(
            r"(\d{2}:\d{2}:\d{2}\.\d{6})\s+\[(\w+)]\s+(.*?)(?=\d{2}:\d{2}:\d{2}\.\d{6}|\Z)",
            log_content,
            re.DOTALL,
        )

        if entries:
            log(
                f"Found {len(entries)} log entries using pattern 1",
                level="debug",
            )
            return pd.DataFrame(entries, columns=["time", "level", "text"])

        # Pattern 2: Alternative pattern for ANSI colored logs
        result = re.split(r"(\x1b\[0m\d{2}:\d{2}:\d{2}\.\d{6})", log_content)
        parts = [part.strip() for part in result if part.strip()]

        if len(parts) > 1:
            log(
                f"Found {len(parts) // 2} log entries using pattern 2",
                level="debug",
            )
            splitted_log = []
            for i in range(0, len(parts) - 1, 2):
                if i + 1 >= len(parts):
                    break

                time = parts[i].replace(r"\x1b[0m", "")
                level_match = re.search(r"\[(\w+)]", parts[i + 1])
                level = level_match.group(1) if level_match else "INFO"
                text = re.sub(r"^\[\w+]\s*", "", parts[i + 1])
                splitted_log.append((time, level, text))

            return pd.DataFrame(
                splitted_log, columns=["time", "level", "text"]
            )

        # Pattern 3: Simple line-by-line parsing (fallback)
        log("Using fallback log parsing method", level="debug")
        lines = log_content.split("\n")
        entries = []

        for line in lines:
            time_match = re.search(r"^(\d{2}:\d{2}:\d{2}\.\d{6})", line)
            if time_match:
                time = time_match.group(1)
                level_match = re.search(r"\[(\w+)]", line)
                level = level_match.group(1) if level_match else "INFO"
                text = re.sub(
                    r"^\d{2}:\d{2}:\d{2}\.\d{6}\s+\[\w+]\s*", "", line
                )
                entries.append((time, level, text))

        if entries:
            log(
                f"Found {len(entries)} log entries using fallback method",
                level="debug",
            )
            return pd.DataFrame(entries, columns=["time", "level", "text"])

        log("Could not parse DBT log file with any pattern", level="warning")
        return pd.DataFrame(columns=["time", "level", "text"])

    except Exception as e:
        log(f"Error parsing DBT log file: {str(e)}", level="error")
        return pd.DataFrame(columns=["time", "level", "text"])


def _extract_model_execution_status_from_logs(logs_df: pd.DataFrame) -> dict:
    """
    Extract the execution status of each model from the logs DataFrame

    Args:
        logs_df: DataFrame containing parsed log entries

    Returns:
        dict: Dictionary mapping model names to their execution status
    """
    model_status = {}

    model_start_pattern = r"Running model ([a-zA-Z0-9_.]+)"
    model_error_pattern = r"Compilation Error in model ([a-zA-Z0-9_.]+)"
    model_success_pattern = r"OK (model|test) ([a-zA-Z0-9_.]+)"

    for _, row in logs_df.iterrows():
        match = re.search(model_start_pattern, row["text"])
        if match:
            model_name = match.group(1)
            model_status[model_name] = "running"

        match = re.search(model_error_pattern, row["text"])
        if match:
            model_name = match.group(1)
            model_status[model_name] = "error"

        match = re.search(model_success_pattern, row["text"])
        if match:
            model_name = match.group(2) if match.group(2) else match.group(1)
            model_status[model_name] = "success"

        if "FAIL" in row["text"].upper():
            for model_name in model_status.keys():
                if model_name in row["text"]:
                    model_status[model_name] = "fail"

    return model_status


def _log_dbt_from_file(log_path) -> dict:
    """
    Process a dbt log file and log its contents with appropriate log levels.
    This function is designed to be the main entry point for log file parsing.

    Args:
        log_path (str): Path to the dbt log file

    Returns:
        dict: Summary statistics about the logs (error count, warning count, etc.)
    """
    logs_df = _process_dbt_log_file(log_path)

    if logs_df.empty:
        log("No log entries found in DBT log file", level="warning")
        return {"error_count": 0, "warning_count": 0, "success": False}

    error_count = 0
    warning_count = 0

    error_logs = logs_df[
        logs_df.level.str.upper().isin(["ERROR", "CRITICAL", "FATAL"])
    ]
    for _, row in error_logs.iterrows():
        log(f"DBT ERROR [{row['time']}]: {row['text']}", level="error")
        error_count += 1

    warning_logs = logs_df[logs_df.level.str.upper().isin(["WARNING", "WARN"])]
    for _, row in warning_logs.iterrows():
        log(f"DBT WARNING [{row['time']}]: {row['text']}", level="warning")
        warning_count += 1

    important_keywords = [
        "success",
        "fail",
        "error",
        "complete",
        "finished",
        "started",
        "running",
        "model",
    ]
    for _, row in logs_df[logs_df.level.str.upper() == "INFO"].iterrows():
        if any(
            keyword in row["text"].lower() for keyword in important_keywords
        ):
            log(f"DBT INFO [{row['time']}]: {row['text']}", level="info")

    success = error_count == 0

    if error_count > 0:
        log(
            f"DBT execution completed with {error_count} errors and {warning_count} warnings",
            level="error",
        )
    elif warning_count > 0:
        log(
            f"DBT execution completed with {warning_count} warnings",
            level="warning",
        )
    else:
        log(
            "DBT execution completed successfully with no errors or warnings",
            level="info",
        )

    return {
        "error_count": error_count,
        "warning_count": warning_count,
        "success": success,
    }


@task
def run_dbt(
    dataset_id: str,
    table_id: str | None = None,
    dbt_alias: bool = True,
    dbt_command: str = "run",
    target: str = "dev",
    flags: str | None = None,
    _vars: dict[str, Any] | str | None = None,
    disable_elementary: bool = False,
) -> bool:
    """
    Execute a DBT model and process logs from the log file.

    Args:
        dataset_id (str): Dataset ID of the dbt model.
        table_id (str, optional): Table ID of the dbt model. If None, the
            whole dataset will be run.
        dbt_alias (bool, optional): If True, the model will be run by
            its alias. Defaults to True.
        dbt_command (str, optional): The dbt command to run. Defaults to "run".
        target (str, optional): The dbt target to use. Defaults to "dev".
        flags (Optional[str], optional): Flags to pass to the dbt command. Defaults to None.
        _vars (Optional[Union[dict[str, Any], str]], optional): Variables to pass to
            dbt. Defaults to None.
        disable_elementary (bool, optional): Disable elementary on-run-end hooks. Defaults to False.

    Raises:
        ValueError: If dbt_command is invalid.
        FAIL: If dbt execution fails.
    """
    if dbt_command not in ["run", "test", "run and test", "run/test"]:
        raise ValueError(f"Invalid dbt_command: {dbt_command}")

    models_folder = Path("models") / dataset_id

    if table_id is not None:
        if dbt_alias:
            selected_table = models_folder / f"{dataset_id}__{table_id}.sql"
        else:
            selected_table = models_folder / f"{table_id}.sql"
    else:
        selected_table = models_folder

    # dbtRunner report success when model file dont exists
    # We check if sql file exists
    if not selected_table.is_dir() and not selected_table.exists():
        msg = f"{selected_table.as_posix()} model file dont exists"
        raise Exception(msg)

    if table_id is None and len(list(selected_table.iterdir())) == 0:
        msg = f"{selected_table.as_posix()} is empty"
        raise Exception(msg)

    vars_deserialize = (
        json.loads(_vars)
        if isinstance(_vars, str)
        else (_vars if _vars is not None else {})
    )

    variables = (
        constants.DISABLE_ELEMENTARY_VARS.value
        if disable_elementary and vars_deserialize is None
        else {
            **constants.DISABLE_ELEMENTARY_VARS.value,
            **vars_deserialize,
        }
    )

    commands_to_run = []

    if "run" in dbt_command:
        commands_to_run.append("run")
    if "test" in dbt_command:
        commands_to_run.append("test")

    log_file_path = os.path.join("logs", "dbt.log")

    if target == "prod":
        with open("/credentials-prod/prod.json", "r") as f:
            service_account = json.loads(f.read())
            project_id = service_account["project_id"]
            client_email = service_account["client_email"]
            log(
                f"Service account for prod: project_id: `{project_id}`, client_email: `{client_email}`"
            )

    for cmd in commands_to_run:
        cli_args = [
            cmd,
            "--select",
            selected_table.as_posix(),
            "--target",
            target,
        ]

        if flags and flags.startswith("--full-refresh") and cmd == "run":
            cli_args.insert(1, "--full-refresh")
        elif flags:
            cli_args.extend(flags.split())

        cli_args.extend(["--vars", f"{json.dumps(variables)}"])

        log(f"Executing dbt command: {' '.join(cli_args)}", level="info")

        dbt_runner = dbtRunner()
        result = dbt_runner.invoke(cli_args)

        if result.success:
            log(
                f"DBT runner reports success for {cmd} command",
                level="info",
            )
        else:
            log(
                f"DBT runner reports failure for {cmd} command. {result.result}",
                level="error",
            )

        if os.path.exists(log_file_path):
            log(f"Processing DBT log file: {log_file_path}", level="info")

            logs_df = _process_dbt_log_file(log_file_path)

            if not logs_df.empty:
                log(
                    f"Found {len(logs_df)} log entries in log file",
                    level="info",
                )

                log_summary = _log_dbt_from_file(log_file_path)

                model_status = _extract_model_execution_status_from_logs(
                    logs_df
                )

                if len(model_status) > 0:
                    log(
                        f"Model execution status: {model_status}",
                        level="info",
                    )

                if log_summary["error_count"] > 0 or not result.success:
                    msg = f"DBT '{cmd}' command failed with {log_summary['error_count']} errors. See logs for details."
                    raise Exception(msg)
            else:
                log("No log entries found in log file", level="warning")
        else:
            log(
                f"DBT log file not found at {log_file_path}",
                level="warning",
            )
            if not result.success:
                raise Exception(result.result)

    return True


def _execute_query_in_bigquery(billing_project_id, query, path, location):
    client = _google_client(billing_project_id, from_file=True, reauth=False)
    job = client["bigquery"].query(query)
    while not job.done():
        sleep(1)

    dest_table = job._properties["configuration"]["query"]["destinationTable"]
    dest_project_id = dest_table["projectId"]
    dest_dataset_id = dest_table["datasetId"]
    dest_table_id = dest_table["tableId"]
    log(
        f"Query results were stored in {dest_project_id}.{dest_dataset_id}.{dest_table_id}"
    )
    blob_path = path
    log(f"Loading data to {blob_path}")
    dataset_ref = bigquery.DatasetReference(dest_project_id, dest_dataset_id)
    table_ref = dataset_ref.table(dest_table_id)
    job_config = bigquery.job.ExtractJobConfig(compression="GZIP")
    extract_job = client["bigquery"].extract_table(
        table_ref,
        blob_path,
        location=location,
        job_config=job_config,
    )
    return extract_job.result()


@task
def download_data_to_gcs(
    dataset_id: str,
    table_id: str,
    project_id: str | None = None,
    query: Union[str, jinja2.Template] | None = None,
    bd_project_mode: str = "prod",
    billing_project_id: str | None = None,
    location: str = "US",
):
    """
    Get data from BigQuery.

    As regras de negócio são:
        - Se a tabela for maior que 5GB: Não tem download disponível
        - Se a tabela for entre 100MB e 5GB: Tem downalod apenas para assinante BDPro
        - Se a tabela for menor que 100MB: Tem download para assinante BDPro e aberto
            - Se for parcialmente BDPro faz o download de arquivos diferentes para o público pagante e não pagante

    """
    # Try to get project_id from environment variable
    if not project_id:
        log(
            "Project ID was not provided, trying to get it from environment variable"
        )
        try:
            bd_base = Base()
            project_id = bd_base.config["gcloud-projects"][bd_project_mode][
                "name"
            ]
        except KeyError:
            pass
        if not project_id:
            raise ValueError(
                "project_id must be either provided or inferred from environment variables"
            )
        log(
            f"Project ID was inferred from environment variables: {project_id}"
        )

    # Asserts that dataset_id and table_id are provided
    if not dataset_id or not table_id:
        raise ValueError("dataset_id and table_id must be provided")

    # If query is not provided, build query from it
    if not query:
        query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"
        log(f"Query was inferred from dataset_id and table_id: {query}")

    # If query is not a string, raise an error
    if not isinstance(query, str):
        raise ValueError("query must be either a string or a Jinja2 template")
    log(f"Query was provided: {query}")

    # Get billing project ID
    if not billing_project_id:
        log(
            "Billing project ID was not provided, trying to get it from environment variable"
        )
        try:
            bd_base = Base()
            billing_project_id = bd_base.config["gcloud-projects"][
                bd_project_mode
            ]["name"]
        except KeyError:
            pass
        if not billing_project_id:
            raise ValueError(
                "billing_project_id must be either provided or inferred from environment variables"
            )
        log(
            f"Billing project ID was inferred from environment variables: {billing_project_id}"
        )

    # pylint: disable=E1124
    client = _google_client(billing_project_id, from_file=True, reauth=False)

    bq_table_ref = TableReference.from_string(
        f"{project_id}.{dataset_id}.{table_id}"
    )
    bq_table = client["bigquery"].get_table(table=bq_table_ref)
    num_bytes = bq_table.num_bytes
    log(f"Quantidade de linhas iniciais {bq_table.num_rows}")
    if num_bytes == 0:
        log("This table is views!")
        b = bd.Backend(graphql_url=get_url("prod"))
        django_table_id = b._get_table_id_from_name(
            gcp_dataset_id=dataset_id, gcp_table_id=table_id
        )
        query_graphql = f"""
        query get_bytes_table{{
        allTable(id: "{django_table_id}"){{
            edges{{
            node{{
                name
                uncompressedFileSize
                        }}}}
                    }}}}
            """
        log(query)
        data = b._execute_query(query_graphql, {"table_id": table_id})
        nodes = data["allTable"]["items"]
        if len(nodes) == 0:
            return None

        num_bytes = nodes[0]["uncompressedFileSize"]

    url_path = get_credentials_from_secret("url_download_data")
    secret_path_url_free = url_path["URL_DOWNLOAD_OPEN"]
    secret_path_url_closed = url_path["URL_DOWNLOAD_CLOSED"]

    log(num_bytes)
    if num_bytes > 1_000_000_000:
        log("Table is bigger than 1GB it is not in the download criteria")
        return None

    if (
        100_000_000 <= num_bytes <= 1_000_000_000
    ):  # Entre 1 GB e 100 MB, apenas BD pro
        log("Querying data for BDpro user")

        blob_path = f"{secret_path_url_closed}{dataset_id}/{table_id}/{table_id}_bdpro.csv.gz"
        _execute_query_in_bigquery(
            billing_project_id=billing_project_id,
            query=query,
            path=blob_path,
            location=location,
        )

        log("BDPro Data was loaded successfully")

    if num_bytes < 100_000_000:  # valor menor que 100 MB
        # Try to remove bdpro access
        log("Trying to remove BDpro filter")
        try:
            query_remove_bdpro = f"DROP ROW ACCESS POLICY bdpro_filter ON `{project_id}.{dataset_id}.{table_id}`"
            log(query_remove_bdpro)
            job = client["bigquery"].query(query_remove_bdpro)
            while not job.done():
                sleep(1)

            log(job.result())

            log(
                "Table has BDpro filter and it was removed so direct download contains only open rows"
            )

            bdpro = True
        except NotFound as e:
            if "Not found: Row access policy bdpro_filter on table" in str(e):
                log(
                    "It was not possible to find BDpro filter, all rows will be downloaded"
                )
                bdpro = False
            else:
                raise (e)
        except Exception as e:
            raise ValueError(e)

        log("Querying open data from BigQuery")

        blob_path = (
            f"{secret_path_url_free}{dataset_id}/{table_id}/{table_id}.csv.gz"
        )
        _execute_query_in_bigquery(
            billing_project_id=billing_project_id,
            query=query,
            path=blob_path,
            location=location,
        )

        log("Open data was loaded successfully")

        if bdpro:
            query_restore_bdpro_access = f'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter  ON  `{project_id}.{dataset_id}.{table_id}` GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org") FILTER USING (TRUE)'

            log(query_restore_bdpro_access)
            job = client["bigquery"].query(query_restore_bdpro_access)

            while not job.done():
                sleep(1)
            log("BDpro filter was reestored")

            log("Querying Data closed from BigQuery")

            blob_path = f"{secret_path_url_closed}{dataset_id}/{table_id}/{table_id}_bdpro.csv.gz"
            _execute_query_in_bigquery(
                billing_project_id=billing_project_id,
                query=query,
                path=blob_path,
                location=location,
            )
            log("Data closed was loaded successfully")
