"""
Tasks for botdosdados
"""
import os
from typing import Tuple
from datetime import timedelta, datetime
from time import sleep

import tweepy
from prefect import task
from basedosdados.download.metadata import _safe_fetch
import pandas as pd
from pipelines.utils.utils import log
from pipelines.datasets.botdosdados.utils import (
    get_credentials_from_secret,
)
from pipelines.constants import constants


# pylint: disable=C0103
@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def echo(message: str) -> None:
    """
    Logs message as a Task
    """
    log(message)


@task(checkpoint=False, nout=5)
def get_credentials(secret_path: str) -> Tuple[str, str, str, str, str]:
    """
    Returns the user and password for the given secret path.
    """
    log(f"Getting user and password for secret path: {secret_path}")
    tokens_dict = get_credentials_from_secret(secret_path)
    access_token_secret = tokens_dict["ACCESS_SECRET"]
    access_token = tokens_dict["ACCESS_TOKEN"]
    consumer_key = tokens_dict["CONSUMER_KEY"]
    consumer_secret = tokens_dict["CONSUMER_SECRET"]
    twitter_token = tokens_dict["TWITTER_TOKEN"]

    return (
        access_token_secret,
        access_token,
        consumer_key,
        consumer_secret,
        twitter_token,
    )


# pylint: disable=R0914
@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def was_table_updated(page_size: int, hours: int) -> bool:
    """
    Checks if there are tables updated within last hour. If True, saves table locally.
    """
    url = f"https://basedosdados.org/api/3/action/bd_dataset_search?q=&resource_type=bdm_table&page=1&page_size={page_size}"
    response = _safe_fetch(url)
    json_response = response.json()
    datasets = json_response["result"]["datasets"]
    n_datasets = len(datasets)
    dfs = []
    for index in range(n_datasets):
        dataset_dict = datasets[index]
        dataset_name = dataset_dict["name"]
        n_tables = len(dataset_dict["resources"])
        tables = [
            dataset_dict["resources"][k]["name"]
            for k in range(n_tables)
            if dataset_dict["resources"][k]["resource_type"] == "bdm_table"
        ]
        metadata_modified = [
            dataset_dict["resources"][k]["metadata_modified"]
            for k in range(n_tables)
            if dataset_dict["resources"][k]["resource_type"] == "bdm_table"
        ]
        df = pd.DataFrame({"table": tables, "metadata_modified": metadata_modified})
        df["dataset"] = dataset_name
        df = df.reindex(["dataset", "table", "metadata_modified"], axis=1)
        dfs.append(df)

    df = dfs[0].append(dfs[1:])
    df["metadata_modified"] = pd.to_datetime(df["metadata_modified"])
    df.sort_values("metadata_modified", ascending=False, inplace=True)
    df = df[df["metadata_modified"] > datetime.now() - pd.Timedelta(hours=hours)]

    if not df.empty:
        os.system("mkdir -p /tmp/data/")
        df.to_csv("/tmp/data/updated_tables.csv")
        return True
    return False


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def send_tweet(
    access_token: str,
    access_token_secret: str,
    consumer_key: str,
    consumer_secret: str,
    bearer_token: str,
):
    """
    Sends one tweet for each new table added recently. Uses 10 seconds interval for each new tweet
    """

    client = tweepy.Client(
        bearer_token=bearer_token,
        consumer_key=consumer_key,
        consumer_secret=consumer_secret,
        access_token=access_token,
        access_token_secret=access_token_secret,
    )

    df = pd.read_csv("/tmp/data/updated_tables.csv")
    dict_updated_tables = dict(zip(df["dataset"].to_list(), df["table"].to_list()))

    for dataset, table in dict_updated_tables.items():
        client.create_tweet(
            text=f"A tabela {table} do dataset {dataset} acaba de ser atualizada no Data Lake da @basedosdados"
        )
        sleep(10)
