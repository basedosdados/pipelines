# -*- coding: utf-8 -*-
"""
General utilities for all pipelines.
"""
import base64
import json

# pylint: disable=too-many-arguments
import logging
from datetime import datetime
from os import getenv, walk
from os.path import join
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from uuid import uuid4

import basedosdados as bd
import croniter
import hvac
import numpy as np
import pandas as pd
import prefect
import requests
from google.cloud import storage
from google.cloud.storage.blob import Blob
from google.oauth2 import service_account
from prefect.client import Client
from prefect.engine.state import State
from prefect.run_configs import KubernetesRun, VertexRun
from redis_pal import RedisPal

from pipelines.constants import constants

import os
import re


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

    blank_spaces = 8 * " "
    msg = blank_spaces + "----\n" + str(msg)
    msg = "\n".join([blank_spaces + line for line in msg.split("\n")]) + "\n\n"

    if level not in levels:
        raise ValueError(f"Invalid log level: {level}")
    prefect.context.logger.log(levels[level], msg)  # pylint: disable=E1101


@prefect.task(checkpoint=False)
def log_task(
    msg: Any, level: str = "info", wait=None  # pylint: disable=unused-argument
):
    """
    Logs a message to prefect's logger.
    """
    log(msg, level)


def get_vault_client() -> hvac.Client:
    """
    Returns a Vault client.
    """
    return hvac.Client(
        url=getenv("VAULT_ADDRESS").strip(),
        token=getenv("VAULT_TOKEN").strip(),
    )


def get_vault_secret(secret_path: str, client: hvac.Client = None) -> dict:
    """
    Returns a secret from Vault.
    """
    vault_client = client if client else get_vault_client()
    return vault_client.secrets.kv.read_secret_version(secret_path)["data"]


def get_credentials_from_secret(
    secret_path: str,
    client: hvac.Client = None,
) -> dict:
    """
    Returns a username and password from a secret in Vault.
    """
    secret = get_vault_secret(secret_path, client)
    return secret["data"]


def set_default_parameters(
    flow: prefect.Flow, default_parameters: dict
) -> prefect.Flow:
    """
    Sets default parameters for a flow.
    """
    for parameter in flow.parameters():
        if parameter.name in default_parameters:
            parameter.default = default_parameters[parameter.name]
    return flow


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


def run_cloud(
    flow: prefect.Flow,
    labels: List[str],
    parameters: Dict[str, Any] = None,
    run_description: str = "",
    agent_type: str = "kubernetes",
    machine_type: str = "f1-micro",
    image: str = "ghcr.io/basedosdados/prefect-flows:latest",
):
    """
    Runs a flow on Prefect Server (must have VPN configured).
    """
    # Setup no schedule
    flow.schedule = None

    # Change flow name for development and register
    flow.name = f"{flow.name} (development)"

    if agent_type == "kubernetes":
        flow.run_config = KubernetesRun(image=image)
    elif agent_type == "vertex":
        flow.run_config = VertexRun(
            image=image,
            machine_type=machine_type,
        )
    else:
        raise ValueError(f"Invalid agent type: {agent_type}")
    flow_id = flow.register(project_name="staging", labels=[])

    # Get Prefect Client and submit flow run
    client = Client()
    flow_run_id = client.create_flow_run(
        flow_id=flow_id,
        run_name=f"TEST RUN - {run_description} - {flow.name}",
        labels=labels,
        parameters=parameters,
    )

    # Print flow run link so user can check it
    print(f"Run submitted: TEST RUN - {run_description} - {flow.name}")
    print(f"Please check at: https://prefect.basedosdados.org/flow-run/{flow_run_id}")


def query_to_line(query: str) -> str:
    """
    Converts a query to a line.
    """
    return " ".join([line.strip() for line in query.split("\n")])


def send_discord_message(
    message: str,
    webhook_url: str,
) -> None:
    """
    Sends a message to a Discord channel.
    """
    requests.post(
        webhook_url,
        data={"content": message},
        timeout=5,
    )


def notify_discord_on_failure(
    flow: prefect.Flow,
    state: State,
    secret_path: str,
    code_owners: Optional[List[str]] = None,
):
    """
    Notifies a Discord channel when a flow fails.
    """
    # url = get_vault_secret(secret_path)["data"]["url"]
    # flow_run_id = prefect.context.get("flow_run_id")
    code_owners = code_owners or constants.DEFAULT_CODE_OWNERS.value
    code_owner_dict = constants.OWNERS_DISCORD_MENTIONS.value
    at_code_owners = []
    for code_owner in code_owners:
        code_owner_id = code_owner_dict[code_owner]["user_id"]
        code_owner_type = code_owner_dict[code_owner]["type"]

        if code_owner_type == "user":
            at_code_owners.append(f"    - <@{code_owner_id}>\n")
        elif code_owner_type == "user_nickname":
            at_code_owners.append(f"    - <@!{code_owner_id}>\n")
        elif code_owner_type == "channel":
            at_code_owners.append(f"    - <#{code_owner_id}>\n")
        elif code_owner_type == "role":
            at_code_owners.append(f"    - <@&{code_owner_id}>\n")

    # message = (
    #     f":man_facepalming: Flow **{flow.name}** has failed."
    #     + f'\n  - State message: *"{state.message}"*'
    #     + "\n  - Link to the failed flow: "
    #     + f"https://prefect.basedosdados.org/flow-run/{flow_run_id}"
    #     + "\n  - Extra attention:\n"
    #     + "".join(at_code_owners)
    # )
    # send_discord_message(
    #     message=message,
    #     webhook_url=url,
    # )


def smart_split(
    text: str,
    max_length: int,
    separator: str = " ",
) -> List[str]:
    """
    Splits a string into a list of strings.
    """
    if len(text) <= max_length:
        return [text]

    separator_index = text.rfind(separator, 0, max_length)
    if (separator_index >= max_length) or (separator_index == -1):
        raise ValueError(
            f'Cannot split text "{text}" into {max_length}'
            f'characters using separator "{separator}"'
        )

    return [
        text[:separator_index],
        *smart_split(
            text[separator_index + len(separator) :],
            max_length,
            separator,
        ),
    ]


def untuple_clocks(clocks):
    """
    Converts a list of tuples to a list of clocks.
    """
    return [clock[0] if isinstance(clock, tuple) else clock for clock in clocks]


###############
#
# Text formatting
#
###############


def human_readable(
    value: Union[int, float],
    unit: str = "",
    unit_prefixes: List[str] = None,
    unit_divider: int = 1000,
    decimal_places: int = 2,
):
    """
    Formats a value in a human readable way.
    """
    if unit_prefixes is None:
        unit_prefixes = ["", "k", "M", "G", "T", "P", "E", "Z", "Y"]
    if value == 0:
        return f"{value}{unit}"
    unit_prefix = unit_prefixes[0]
    for prefix in unit_prefixes[1:]:
        if value < unit_divider:
            break
        unit_prefix = prefix
        value /= unit_divider
    return f"{value:.{decimal_places}f}{unit_prefix}{unit}"


###############
#
# Dataframe
#
###############


def dataframe_to_csv(dataframe: pd.DataFrame, path: Union[str, Path]) -> None:
    """
    Writes a dataframe to a CSV file.
    """
    # Remove filename from path
    path = Path(path)
    # Create directory if it doesn't exist
    path.parent.mkdir(parents=True, exist_ok=True)
    # Write dataframe to CSV
    dataframe.to_csv(path, index=False, encoding="utf-8")


def batch_to_dataframe(batch: Tuple[Tuple], columns: List[str]) -> pd.DataFrame:
    """
    Converts a batch of rows to a dataframe.
    """
    return pd.DataFrame(batch, columns=columns)


def clean_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans a dataframe.
    """
    for col in dataframe.columns.tolist():
        if dataframe[col].dtype == object:
            try:
                dataframe[col] = (
                    dataframe[col]
                    .astype(str)
                    .str.replace("\x00", "", regex=True)
                    .replace("None", np.nan, regex=True)
                )
            except Exception as exc:
                print("Column: ", col, "\nData: ", dataframe[col].tolist(), "\n", exc)
                raise
    return dataframe


def remove_columns_accents(dataframe: pd.DataFrame) -> list:
    """
    Remove accents from dataframe columns.
    """
    return list(
        dataframe.columns.str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
    )


def to_partitions(
    data: pd.DataFrame,
    partition_columns: List[str],
    savepath: str,
    file_type: str = "csv",
):
    """Save data in to hive patitions schema, given a dataframe and a list of partition columns.
    Args:
        data (pandas.core.frame.DataFrame): Dataframe to be partitioned.
        partition_columns (list): List of columns to be used as partitions.
        savepath (str, pathlib.PosixPath): folder path to save the partitions.
        file_type (str): default to csv. Accepts parquet.
    Exemple:
        data = {
            "ano": [2020, 2021, 2020, 2021, 2020, 2021, 2021,2025],
            "mes": [1, 2, 3, 4, 5, 6, 6,9],
            "sigla_uf": ["SP", "SP", "RJ", "RJ", "PR", "PR", "PR","PR"],
            "dado": ["a", "b", "c", "d", "e", "f", "g",'h'],
        }
        to_partitions(
            data=pd.DataFrame(data),
            partition_columns=['ano','mes','sigla_uf'],
            savepath='partitions/',
        )
    """

    if isinstance(data, (pd.core.frame.DataFrame)):
        savepath = Path(savepath)
        # create unique combinations between partition columns
        unique_combinations = (
            data[partition_columns]
            # .astype(str)
            .drop_duplicates(subset=partition_columns).to_dict(orient="records")
        )

        for filter_combination in unique_combinations:
            patitions_values = [
                f"{partition}={value}"
                for partition, value in filter_combination.items()
            ]

            # get filtered data
            df_filter = data.loc[
                data[filter_combination.keys()]
                .isin(filter_combination.values())
                .all(axis=1),
                :,
            ]
            df_filter = df_filter.drop(columns=partition_columns)

            # create folder tree
            filter_save_path = Path(savepath / "/".join(patitions_values))
            filter_save_path.mkdir(parents=True, exist_ok=True)

            if file_type == "csv":
                # append data to csv
                file_filter_save_path = Path(filter_save_path) / "data.csv"
                df_filter.to_csv(
                    file_filter_save_path,
                    sep=",",
                    encoding="utf-8",
                    na_rep="",
                    index=False,
                    mode="a",
                    header=not file_filter_save_path.exists(),
                )
            elif file_type == "parquet":
                # append data to parquet
                file_filter_save_path = Path(filter_save_path) / "data.parquet"
                df_filter.to_parquet(
                    file_filter_save_path, index=False, compression="gzip"
                )
    else:
        raise BaseException("Data need to be a pandas DataFrame")


###############
#
# Storage utils
#
###############


def get_storage_blobs(dataset_id: str, table_id: str) -> list:
    """
    Get all blobs from a table in a dataset.
    """

    storage_bd = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    return list(
        storage_bd.client["storage_staging"]
        .bucket(storage_bd.bucket_name)
        .list_blobs(prefix=f"staging/{storage_bd.dataset_id}/{storage_bd.table_id}/")
    )


def parser_blobs_to_partition_dict(blobs: list) -> dict:
    """
    Extracts the partition information from the blobs.
    """

    partitions_dict = {}
    for blob in blobs:
        for folder in blob.name.split("/"):
            if "=" in folder:
                key = folder.split("=")[0]
                value = folder.split("=")[1]
                try:
                    partitions_dict[key].append(value)
                except KeyError:
                    partitions_dict[key] = [value]
    return partitions_dict


def dump_header_to_csv(data_path: Union[str, Path], source_format: str):
    """
    Writes the header of a CSV or Parquet file to a new file.

    Parameters:
        data_path (Union[str, Path]): The path to the directory containing CSV or Parquet files.
        source_format (str): The source format, either 'csv' or 'parquet'.

    Returns:
        str: The path where the header file is saved.

    Raises:
        FileNotFoundError: If no CSV or Parquet file is found in the specified directory.

    Note:
        The function will search for either a CSV or Parquet file in the provided data_path.
        If the file is found, it will read the first row (header) and save it to a new file
        with the format 'header.csv' for CSV files and 'header.parquet' for Parquet files.
        If the data_path contains partition folders (folders with '=' in their names), the
        header file will be saved in a corresponding partition path under a 'data' directory.
    """
    # Remove filename from path
    path = Path(data_path)
    if not path.is_dir():
        path = path.parent
    # Grab first CSV or Parquet file found
    found: bool = False
    file: str = None
    for subdir, _, filenames in walk(str(path)):
        for fname in filenames:
            if source_format == "csv":
                if fname.endswith(".csv"):
                    file = join(subdir, fname)
                    log(f"Found CSV file: {file}")
                    found = True
                    break

            elif source_format == "parquet":
                if fname.endswith(".parquet"):
                    file = join(subdir, fname)
                    log(f"Found Parquet file: {file}")
                    found = True
                    break
        if found:
            break

    if file is None:
        raise FileNotFoundError("No parquet or csv file found")

    save_header_path = f"data/{uuid4()}"
    # discover if it's a partitioned table
    if partition_folders := [folder for folder in file.split("/") if "=" in folder]:
        partition_path = "/".join(partition_folders)
        if source_format == "csv":
            save_header_file_path = Path(
                f"{save_header_path}/{partition_path}/header.csv"
            )
            log(f"Found partition path: {save_header_file_path}")
        elif source_format == "parquet":
            save_header_file_path = Path(
                f"{save_header_path}/{partition_path}/header.parquet"
            )
            log(f"Found partition path: {save_header_file_path}")

    else:
        if source_format == "csv":
            save_header_file_path = Path(f"{save_header_path}/header.csv")
            log(f"Do not found partition path: {save_header_file_path}")
        elif source_format == "parquet":
            save_header_file_path = Path(f"{save_header_path}/header.parquet")
            log(f"Do not found partition path: {save_header_file_path}")

    # Create directory if it doesn't exist
    save_header_file_path.parent.mkdir(parents=True, exist_ok=True)

    # Read just first row
    if source_format == "csv":
        dataframe = pd.read_csv(file, nrows=1)

        # Write dataframe to CSV
        dataframe.to_csv(save_header_file_path, index=False, encoding="utf-8")
    elif source_format == "parquet":
        dataframe = pd.read_parquet(file)
        dataframe = dataframe.head(1)

        # Write dataframe to Parquet
        dataframe.to_parquet(save_header_file_path, index=False)

    log(f"Wrote header Parquet: {save_header_file_path}")
    return save_header_path


def determine_whether_to_execute_or_not(
    cron_expression: str, datetime_now: datetime, datetime_last_execution: datetime
) -> bool:
    """
    Determines whether the cron expression is currently valid.
    Args:
        cron_expression: The cron expression to check.
        datetime_now: The current datetime.
        datetime_last_execution: The last datetime the cron expression was executed.
    Returns:
        True if the cron expression should trigger, False otherwise.
    """
    cron_expression_iterator = croniter.croniter(
        cron_expression, datetime_last_execution
    )
    next_cron_expression_time = cron_expression_iterator.get_next(datetime)
    if next_cron_expression_time <= datetime_now:
        return True
    return False


def get_redis_client(
    host: str = "redis.redis.svc.cluster.local",
    port: int = 6379,
    db: int = 0,  # pylint: disable=C0103
    password: str = None,
) -> RedisPal:
    """
    Returns a Redis client.
    """
    return RedisPal(
        host=host,
        port=port,
        db=db,
        password=password,
    )


def list_blobs_with_prefix(
    bucket_name: str, prefix: str, mode: str = "prod"
) -> List[Blob]:
    """
    Lists all the blobs in the bucket that begin with the prefix.
    This can be used to list all blobs in a "folder", e.g. "public/".
    Mode needs to be "prod" or "staging"
    """

    credentials = get_credentials_from_env(mode=mode)
    storage_client = storage.Client(credentials=credentials)

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)

    return list(blobs)


def get_credentials_from_env(
    mode: str = "prod", scopes: List[str] = None
) -> service_account.Credentials:
    """
    Gets credentials from env vars
    """
    if mode not in ["prod", "staging"]:
        raise ValueError("Mode must be 'prod' or 'staging'")
    env: str = getenv(f"BASEDOSDADOS_CREDENTIALS_{mode.upper()}", "")
    if env == "":
        raise ValueError(f"BASEDOSDADOS_CREDENTIALS_{mode.upper()} env var not set!")
    info: dict = json.loads(base64.b64decode(env))
    cred: service_account.Credentials = (
        service_account.Credentials.from_service_account_info(info)
    )
    if scopes:
        cred = cred.with_scopes(scopes)
    return cred


#######################
# Django Metadata Utils
#######################
def get_first_date(
    ids,
    email,
    password,
    date_format: str,
    api_mode: str = "prod",
):
    """
    Retrieves the first date from the given coverage ID.

    Args:
        ids (dict): A dictionary containing the dataset ID, table ID, and coverage ID.
        date_format (str): Date format ('yy-mm-dd', 'yy-mm', or 'yy')
    Returns:
        str: The first date in the format 'YYYY-MM-DD(interval)'.

    Raises:
        Exception: If an error occurs while retrieving the first date.
    """
    try:
        date = get_date(
            query_class="allDatetimerange",
            query_parameters={"$coverage_Id: ID": ids.get("coverage_id")},
            email=email,
            password=password,
            api_mode=api_mode,
        )
        data = date["data"]["allDatetimerange"]["edges"][0]["node"]
        log(data)
        if date_format == "yy-mm-dd":
            first_date = f"{data['startYear']}-{data['startMonth']}-{data['startDay']}({data['interval']})"
        elif date_format == "yy-mm":
            first_date = f"{data['startYear']}-{data['startMonth']}({data['interval']})"
        elif date_format == "yy":
            first_date = f"{data['startYear']}({data['interval']})"

        log(f"Primeira data: {first_date}")
        return first_date
    except Exception as e:
        log(f"An error occurred while retrieving the first date: {str(e)}")
        raise


def extract_last_update(
    dataset_id, table_id, date_format: str, billing_project_id: str
):
    """
    Extracts the last update date of a given dataset table.

    Args:
        dataset_id (str): The ID of the dataset.
        table_id (str): The ID of the table.
        date_format (str): Date format ('yy-mm-dd', 'yy-mm', or 'yy')

    Returns:
        str: The last update date in the format 'YYYY-MM-DD'.

    Raises:
        Exception: If an error occurs while extracting the last update date.
    """
    try:
        query_bd = f"""
        SELECT
        *
        FROM
        `basedosdados.{dataset_id}.__TABLES__`
        WHERE
        table_id = '{table_id}'
        """

        t = bd.read_sql(
            query=query_bd,
            billing_project_id=billing_project_id,
            from_file=True,
        )
        timestamp = (
            t["last_modified_time"][0] / 1000
        )  # Convert to seconds by dividing by 1000
        dt = datetime.fromtimestamp(timestamp)
        if date_format == "yy-mm-dd":
            last_date = dt.strftime("%Y-%m-%d")
        elif date_format == "yy-mm":
            last_date = dt.strftime("%Y-%m")
        elif date_format == "yy":
            last_date = dt.strftime("%Y")
        log(f"Última data: {last_date}")
        return last_date
    except Exception as e:
        log(f"An error occurred while extracting the last update date: {str(e)}")
        raise


def extract_last_date(dataset_id, table_id, date_format: str, billing_project_id: str):
    """
    Extracts the last update date of a given dataset table.

    Args:
        dataset_id (str): The ID of the dataset.
        table_id (str): The ID of the table.
        date_format (str): Date format ('yy-mm' or 'yy-mm-dd')
        if set to 'yy-mm' the function will look for  ano and mes named columns in the table_id
        and return a concatenated string in the formar yyyy-mm. if set to 'yyyy-mm-dd'
        the function will look for  data named column in the format 'yyyy-mm-dd' and return it.

    Returns:
        str: The last update date in the format 'yyyy-mm' or 'yyyy-mm-dd'.

    Raises:
        Exception: If an error occurs while extracting the last update date.
    """
    if date_format == "yy-mm":
        try:
            query_bd = f"""
            SELECT
            MAX(CONCAT(ano,"-",mes)) as max_date
            FROM
            `{billing_project_id}.{dataset_id}.{table_id}`
            """

            t = bd.read_sql(
                query=query_bd,
                billing_project_id=billing_project_id,
                from_file=True,
            )
            input_date_str = t["max_date"][0]

            date_obj = datetime.strptime(input_date_str, "%Y-%m")

            last_date = date_obj.strftime("%Y-%m")
            log(f"Última data YYYY-MM: {last_date}")

            return last_date
        except Exception as e:
            log(f"An error occurred while extracting the last update date: {str(e)}")
            raise
    else:
        try:
            query_bd = f"""
            SELECT
            MAX(data) as max_date
            FROM
            `basedosdados.{dataset_id}.{table_id}`
            """
            log(f"Query: {query_bd}")
            t = bd.read_sql(
                query=query_bd,
                billing_project_id=billing_project_id,
                from_file=True,
            )
            # it infers that the data variable is already on basedosdados standart format
            # yyyy-mm-dd
            last_date = t["max_date"][0]

            log(f"Última data YYYY-MM-DD: {last_date}")

            return last_date
        except Exception as e:
            log(f"An error occurred while extracting the last update date: {str(e)}")
            raise


def find_ids(dataset_id, table_id, email, password):
    """
    Finds the IDs for a given dataset and table.

    Args:
        dataset_id (str): The ID of the dataset.
        table_id (str): The ID of the table.

    Returns:
        dict: A dictionary containing the dataset ID, table ID, and coverage ID.

    Raises:
        Exception: If an error occurs while retrieving the IDs.
    """
    try:
        ids = get_ids(
            dataset_name=dataset_id, table_name=table_id, email=email, password=password
        )
        log(f"IDs >>>> {ids}")
        return ids
    except Exception as e:
        log(f"An error occurred while retrieving the IDs: {str(e)}")
        raise


def get_credentials_utils(secret_path: str) -> Tuple[str, str]:
    """
    Returns the user and password for the given secret path.
    """
    log(f"Getting user and password for secret path: {secret_path}")
    tokens_dict = get_credentials_from_secret(secret_path)
    email = tokens_dict.get("email")
    password = tokens_dict.get("password")
    return email, password


def get_token(email, password, api_mode: str = "prod"):
    """
    Get api token.
    """
    r = None
    if api_mode == "prod":
        r = requests.post(
            "http://api.basedosdados.org/api/v1/graphql",
            headers={"Content-Type": "application/json"},
            json={
                "query": """
            mutation ($email: String!, $password: String!) {
                tokenAuth(email: $email, password: $password) {
                    token
                }
            }
        """,
                "variables": {"email": email, "password": password},
            },
        )

        r.raise_for_status()
    elif api_mode == "staging":
        r = requests.post(
            "http://staging.api.basedosdados.org/api/v1/graphql",
            headers={"Content-Type": "application/json"},
            json={
                "query": """
            mutation ($email: String!, $password: String!) {
                tokenAuth(email: $email, password: $password) {
                    token
                }
            }
        """,
                "variables": {"email": email, "password": password},
            },
        )
        r.raise_for_status()
    return r.json()["data"]["tokenAuth"]["token"]


def get_id(
    query_class,
    query_parameters,
    email,
    password,
    api_mode: str = "prod",
    cloud_table: bool = True,
):
    token = get_token(email, password, api_mode)
    header = {
        "Authorization": f"Bearer {token}",
    }
    _filter = ", ".join(list(query_parameters.keys()))
    keys = [
        parameter.replace("$", "").split(":")[0]
        for parameter in list(query_parameters.keys())
    ]
    values = list(query_parameters.values())
    _input = ", ".join([f"{key}:${key}" for key in keys])

    if cloud_table:
        query = f"""query({_filter}) {{
                            {query_class}({_input}){{
                            edges{{
                                node{{
                                id,
                                table{{
                                    _id
                                    }}
                                }}
                            }}
                            }}
                        }}"""
    else:
        query = f"""query({_filter}) {{
                            {query_class}({_input}){{
                            edges{{
                                node{{
                                id,
                                }}
                            }}
                            }}
                        }}"""

    if api_mode == "staging":
        r = requests.post(
            url=f"https://{api_mode}.api.basedosdados.org/api/v1/graphql",
            json={"query": query, "variables": dict(zip(keys, values))},
            headers=header,
        ).json()
    elif api_mode == "prod":
        r = requests.post(
            url="https://api.basedosdados.org/api/v1/graphql",
            json={"query": query, "variables": dict(zip(keys, values))},
            headers=header,
        ).json()

    if "data" in r and r is not None:
        if r.get("data", {}).get(query_class, {}).get("edges") == []:
            id = None
            # print(f"get: not found {query_class}", dict(zip(keys, values)))
        else:
            id = r["data"][query_class]["edges"][0]["node"]["id"]
            # print(f"get: found {id}")
            id = id.split(":")[1]
        return r, id
    else:
        print("get:  Error:", json.dumps(r, indent=4, ensure_ascii=False))
        raise Exception("get: Error")


def get_date(
    query_class,
    query_parameters,
    email,
    password,
    api_mode: str = "prod",
):
    token = get_token(
        email=email,
        password=password,
        api_mode=api_mode,
    )
    log("puxou token")
    header = {
        "Authorization": f"Bearer {token}",
    }
    log(f"{header}")
    _filter = ", ".join(list(query_parameters.keys()))
    keys = [
        parameter.replace("$", "").split(":")[0]
        for parameter in list(query_parameters.keys())
    ]
    values = list(query_parameters.values())
    _input = ", ".join([f"{key}:${key}" for key in keys])

    query = f"""query({_filter}) {{
                        {query_class}({_input}){{
                        edges{{
                            node{{
                            startYear,
                            startMonth,
                            startDay,
                            interval,
                            }}
                        }}
                        }}
                    }}"""

    if api_mode == "staging":
        r = requests.post(
            url=f"https://{api_mode}.api.basedosdados.org/api/v1/graphql",
            json={"query": query, "variables": dict(zip(keys, values))},
            headers=header,
        ).json()
    elif api_mode == "prod":
        r = requests.post(
            url="https://api.basedosdados.org/api/v1/graphql",
            json={"query": query, "variables": dict(zip(keys, values))},
            headers=header,
        ).json()

    return r


def create_update(
    email,
    password,
    mutation_class,
    mutation_parameters,
    query_class,
    query_parameters,
    update=False,
    api_mode: str = "prod",
):
    token = get_token(
        email=email,
        password=password,
        api_mode=api_mode,
    )
    header = {
        "Authorization": f"Bearer {token}",
    }

    r, id = get_id(
        query_class=query_class,
        query_parameters=query_parameters,
        email=email,
        password=password,
        cloud_table=False,
        api_mode=api_mode,
    )
    if id is not None:
        r["r"] = "query"
        if update is False:
            return r, id

    _classe = mutation_class.replace("CreateUpdate", "").lower()
    query = f"""
                mutation($input:{mutation_class}Input!){{
                    {mutation_class}(input: $input){{
                    errors {{
                        field,
                        messages
                    }},
                    clientMutationId,
                    {_classe} {{
                        id,
                    }}
                }}
                }}
            """

    if update is True and id is not None:
        mutation_parameters["id"] = id

        if api_mode == "prod":
            r = requests.post(
                "https://api.basedosdados.org/api/v1/graphql",
                json={"query": query, "variables": {"input": mutation_parameters}},
                headers=header,
            ).json()
        elif api_mode == "staging":
            r = requests.post(
                f"https://{api_mode}api.basedosdados.org/api/v1/graphql",
                json={"query": query, "variables": {"input": mutation_parameters}},
                headers=header,
            ).json()

    r["r"] = "mutation"
    if "data" in r and r is not None:
        if r.get("data", {}).get(mutation_class, {}).get("errors", []) != []:
            print(f"create: not found {mutation_class}", mutation_parameters)
            print("create: error\n", json.dumps(r, indent=4, ensure_ascii=False), "\n")
            id = None
            raise Exception("create: Error")
        else:
            id = r["data"][mutation_class][_classe]["id"]
            id = id.split(":")[1]

            return r, id
    else:
        print("\n", "create: query\n", query, "\n")
        print(
            "create: input\n",
            json.dumps(mutation_parameters, indent=4, ensure_ascii=False),
            "\n",
        )
        print("create: error\n", json.dumps(r, indent=4, ensure_ascii=False), "\n")
        raise Exception("create: Error")


def parse_temporal_coverage(temporal_coverage):
    padrao_ano = r"\d{4}\(\d{1,2}\)\d{4}"
    padrao_mes = r"\d{4}-\d{2}\(\d{1,2}\)\d{4}-\d{2}"
    padrao_semana = r"\d{4}-\d{2}-\d{2}\(\d{1,2}\)\d{4}-\d{2}-\d{2}"
    padrao_dia = r"\d{4}-\d{2}-\d{2}\(\d{1,2}\)\d{4}-\d{2}-\d{2}"

    if (
        re.match(padrao_ano, temporal_coverage)
        or re.match(padrao_mes, temporal_coverage)
        or re.match(padrao_semana, temporal_coverage)
        or re.match(padrao_dia, temporal_coverage)
    ):
        print("A data está no formato correto.")
    else:
        print("Aviso: A data não está no formato correto.")

    # Extrai as informações de data e intervalo da string
    if "(" in temporal_coverage:
        start_str, interval_str, end_str = re.split(r"[(|)]", temporal_coverage)
        if start_str == "" and end_str != "":
            start_str = end_str
        elif end_str == "" and start_str != "":
            end_str = start_str
    elif len(temporal_coverage) >= 4:
        start_str, interval_str, end_str = temporal_coverage, 1, temporal_coverage
    start_len = 0 if start_str == "" else len(start_str.split("-"))
    end_len = 0 if end_str == "" else len(end_str.split("-"))

    def parse_date(position, date_str, date_len):
        result = {}
        if date_len == 3:
            date = datetime.strptime(date_str, "%Y-%m-%d")
            result[f"{position}Year"] = date.year
            result[f"{position}Month"] = date.month
            result[f"{position}Day"] = date.day
        elif date_len == 2:
            date = datetime.strptime(date_str, "%Y-%m")
            result[f"{position}Year"] = date.year
            result[f"{position}Month"] = date.month
        elif date_len == 1:
            date = datetime.strptime(date_str, "%Y")
            result[f"{position}Year"] = date.year
        return result

    start_result = parse_date(position="start", date_str=start_str, date_len=start_len)
    end_result = parse_date(position="end", date_str=end_str, date_len=end_len)
    start_result.update(end_result)

    if interval_str != 0:
        start_result["interval"] = int(interval_str)

    return end_result


def get_ids(
    dataset_name: str,
    table_name: str,
    email: str,
    password: str,
    api_mode: str = "prod",
) -> dict:
    """
    Obtains the IDs of the table and coverage based on the provided names.
    """
    try:
        # Get the table ID
        table_result = get_id(
            email=email,
            password=password,
            query_class="allCloudtable",
            query_parameters={
                "$gcpDatasetId: String": dataset_name,
                "$gcpTableId: String": table_name,
            },
            cloud_table=True,
            api_mode=api_mode,
        )
        if not table_result:
            raise ValueError("Table ID not found.")

        table_id = table_result[0]["data"]["allCloudtable"]["edges"][0]["node"][
            "table"
        ].get("_id")

        # Get the coverage IDs
        coverage_result = get_id(
            email=email,
            password=password,
            query_class="allCoverage",
            query_parameters={"$table_Id: ID": table_id},
            api_mode=api_mode,
            cloud_table=True,
        )
        if not coverage_result:
            raise ValueError("Coverage ID not found.")

        coverage_ids = coverage_result[0]

        # Check if there are multiple coverage IDs
        if len(coverage_ids["data"]["allCoverage"]["edges"]) > 1:
            print(
                "WARNING: Your table has more than one coverage. Only the first ID has been selected."
            )

        # Retrieve the first coverage ID
        coverage_id = coverage_ids["data"]["allCoverage"]["edges"][0]["node"][
            "id"
        ].split(":")[-1]

        # Return the 2 IDs in a dictionary
        return {
            "table_id": table_id,
            "coverage_id": coverage_id,
        }
    except Exception as e:
        print(f"Error occurred while retrieving IDs: {str(e)}")
        raise
