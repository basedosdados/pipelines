# -*- coding: utf-8 -*-
"""
General utilities for all pipelines.
"""

import base64
import json

# pylint: disable=too-many-arguments
import logging
import zipfile
from datetime import datetime
from io import BytesIO
from os import getenv, walk
from os.path import join
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.request import urlopen
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
    msg: Any,
    level: str = "info",
    wait=None,  # pylint: disable=unused-argument
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
    print(
        f"Please check at: https://prefect.basedosdados.org/flow-run/{flow_run_id}"
    )


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
    url = get_vault_secret(secret_path)["data"]["url"]
    flow_run_id = prefect.context.get("flow_run_id")
    labels = prefect.context.config.cloud.agent.labels
    code_owners = code_owners or constants.DEFAULT_CODE_OWNERS.value
    code_owner_dict = constants.OWNERS_DISCORD_MENTIONS.value

    if labels != ["basedosdados"]:
        return

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

    message = (
        f":man_facepalming: Flow **{flow.name}** has failed."
        + f'\n  - State message: *"{state.message}"*'
        + "\n  - Link to the failed flow: "
        + f"https://prefect.basedosdados.org/flow-run/{flow_run_id}"
        + "\n  - Use this link to document pipeline errors:  https://forms.gle/uhyuHGDahpkgfsXs8"
        + "\n  - Extra attention:\n"
        + "".join(at_code_owners)
    )
    send_discord_message(
        message=message,
        webhook_url=url,
    )


def notify_discord(
    secret_path: str,
    message: str,
    code_owners: Optional[List[str]] = None,
):
    """
    Notifies a Discord channel.
    """
    url = get_vault_secret(secret_path)["data"]["url"]
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

    message = message + "".join(at_code_owners)

    send_discord_message(
        message=message,
        webhook_url=url,
    )


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
            f'Cannot split text "{text}" into {max_length}characters using separator "{separator}"'
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
    return [
        clock[0] if isinstance(clock, tuple) else clock for clock in clocks
    ]


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


def batch_to_dataframe(
    batch: Tuple[Tuple], columns: List[str]
) -> pd.DataFrame:
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
                print(
                    "Column: ",
                    col,
                    "\nData: ",
                    dataframe[col].tolist(),
                    "\n",
                    exc,
                )
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
            .drop_duplicates(subset=partition_columns)
            .to_dict(orient="records")
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
                    file_filter_save_path,
                    index=False,
                    compression="gzip",
                )
    else:
        raise BaseException("Data need to be a pandas DataFrame")


###############
#
# Download data
#
###############


def download_and_unzip_file(url: str, path: str) -> None:
    """
    Downloads a file from the given URL and extracts it to the specified path.

    Parameters:
    url (str): The URL of the file to be downloaded.
    path (str): The path where the file will be extracted.

    Returns:
    None
    """

    log("------------------ Downloading and unzipping file ------------------")
    try:
        r = urlopen(url)
        zip = zipfile.ZipFile(BytesIO(r.read()))
        zip.extractall(path=path)
        log(f"DOWNLOAD {url} to {path} FINISH")

    except Exception as e:
        log(e)
        log("Error when downloading and unzipping file")


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
        .list_blobs(
            prefix=f"staging/{storage_bd.dataset_id}/{storage_bd.table_id}/"
        )
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
    if partition_folders := [
        folder for folder in file.split("/") if "=" in folder
    ]:
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
    cron_expression: str,
    datetime_now: datetime,
    datetime_last_execution: datetime,
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
        raise ValueError(
            f"BASEDOSDADOS_CREDENTIALS_{mode.upper()} env var not set!"
        )
    info: dict = json.loads(base64.b64decode(env))
    cred: service_account.Credentials = (
        service_account.Credentials.from_service_account_info(info)
    )
    if scopes:
        cred = cred.with_scopes(scopes)
    return cred
