"""
Tasks for botdosdados
"""
import os
from typing import Tuple
from datetime import timedelta
from time import sleep

import tweepy
from prefect import task
import pandas as pd
import basedosdados as bd
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
    access_secret = tokens_dict["ACCESS_SECRET"]
    access_token = tokens_dict["ACCESS_TOKEN"]
    consumer_key = tokens_dict["CONSUMER_KEY"]
    consumer_secret = tokens_dict["CONSUMER_SECRET"]
    twitter_token = tokens_dict["TWITTER_TOKEN"]

    return access_secret, access_token, consumer_key, consumer_secret, twitter_token


# pylint: disable=R0914
@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def was_table_updated() -> bool:
    """
    Checks if there are tables updated within last hour. If True, saves table locally.
    """
    df = bd.read_sql(
        """
        SELECT table_catalog as proj, table_schema as dataset, table_name, creation_time
        FROM region-us.INFORMATION_SCHEMA.TABLES
        WHERE creation_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) AND CURRENT_TIMESTAMP()
        ORDER BY creation_time DESC
    """,
        from_file=True,
    )

    if not df.empty:
        os.system("mkdir -p /tmp/data/updated_tables.csv")
        return True
    return False


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def send_tweet(
    access_secret: str,
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
        access_token=access_secret,
        access_token_secret=access_token_secret,
    )

    df = pd.read_csv("/tmp/data/updated_tables.csv")
    dict_updated_tables = dict(df["dataset"].to_list(), df["table_name"].to_list())

    for dataset, table in dict_updated_tables.items():
        client.create_tweet(
            text=f"A tabela {table} do dataset {dataset} acaba de ser atualizada no Data Lake da @basedosdados"
        )
        sleep(10)
