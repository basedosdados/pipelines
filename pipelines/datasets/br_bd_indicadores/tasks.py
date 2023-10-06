# -*- coding: utf-8 -*-
"""
Tasks for br_twitter
"""
# pylint: disable=invalid-name,too-many-branches,too-many-nested-blocks
import os
from datetime import datetime, timedelta
from functools import reduce
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import pandas as pd
import pytz
import requests
from prefect import task
from prefect.triggers import all_successful
from requests_oauthlib import OAuth1
from tqdm import tqdm

from pipelines.constants import constants
from pipelines.datasets.br_bd_indicadores.utils import (
    GA4RealTimeReport,
    connect_to_endpoint,
    create_google_sheet_url,
    create_headers,
    create_url,
    flatten,
    get_report,
    initialize_analyticsreporting,
    parse_data,
)
from pipelines.utils.utils import get_credentials_from_secret, get_storage_blobs, log


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


# pylint: disable=W0613
@task(checkpoint=False, nout=5)
def get_twitter_credentials(
    secret_path: str, wait=None
) -> Tuple[str, str, str, str, str]:
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
def has_new_tweets(bearer_token: str, table_id: str) -> bool:
    """
    Checks if there are new tweets to capture data
    """
    now = datetime.now(tz=pytz.UTC)
    headers = create_headers(bearer_token)

    # non_public_metrics only available for last 30 days
    before = now - timedelta(days=29)
    start_time = before.strftime("%Y-%m-%dT00:00:00.000Z")
    end_time = now.strftime("%Y-%m-%dT00:00:00.000Z")
    max_results = 100
    # pylint: disable=E1121
    url = create_url(start_time, end_time, max_results)
    json_response = connect_to_endpoint(url[0], headers, url[1])
    data = [flatten(i) for i in json_response["data"]]
    df1 = pd.DataFrame(data)

    blobs = get_storage_blobs(dataset_id="br_bd_indicadores", table_id=table_id)
    now = datetime.now(tz=pytz.UTC)

    if len(blobs) != 0:
        dfs = []
        for blob in blobs:
            url_data = blob.public_url
            df = pd.read_csv(url_data, dtype={"id": str})
            dfs.append(df)

        df = dfs[0].append(dfs[1:])
        ids = df.id.to_list()
        df1 = df1[~df1["id"].isin(ids)]

    if len(df1) > 0:
        log(f"{len(df1)} new tweets founded")

    df1.to_csv("/tmp/basic_metrics.csv", index=False)

    return not df1.empty


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def crawler_metricas(
    access_secret: str,
    access_token: str,
    consumer_key: str,
    consumer_secret: str,
    table_id: str,
) -> str:
    """
    Create file with public and non_public_metrics from Twitter API
    """
    df1 = pd.read_csv("/tmp/basic_metrics.csv")
    ids = df1["id"].to_list()

    headeroauth = OAuth1(
        consumer_key,
        consumer_secret,
        access_token,
        access_secret,
        signature_type="auth_header",
    )

    temp_dict = {}
    for id_field in tqdm(ids):
        # retweets don't have non_public_metrics
        if not df1[df1.id == id_field].text.to_list()[0].startswith("RT @"):
            url = f"https://api.twitter.com/2/tweets/{id_field}?tweet.fields=non_public_metrics"

            try:
                r = requests.get(url, auth=headeroauth, timeout=10)

                json_response = r.json()
                temp_dict.update(
                    {id_field: json_response["data"]["non_public_metrics"]}
                )
            except KeyError:
                log(json_response["errors"])
        else:
            temp_dict.update(
                {
                    id_field: {
                        "url_link_clicks": np.nan,
                        "user_profile_clicks": np.nan,
                        "impression_count": np.nan,
                    }
                }
            )

    df2 = pd.DataFrame(temp_dict).T
    df2.columns = ["non_public_metrics_" + k for k in df2.columns]

    df = df1.set_index("id").join(df2)

    df.columns = [
        col.replace("non_public_metrics_", "").replace("public_metrics_", "")
        for col in df.columns
    ]

    # A partir de 10/01/2023 o campo "impression_count" ficou duplicado no dataframe
    # Não consegui encontrar o motivo, então estou removendo o campo duplicado

    df = df.loc[:, ~df.columns.duplicated()]

    url = (
        "https://api.twitter.com/2/users/1184334528837574656?user.fields=public_metrics"
    )
    try:
        r = requests.get(url, auth=headeroauth, timeout=10)
        json_response = r.json()
        result = json_response["data"]["public_metrics"]
    except KeyError:
        log(json_response["errors"])

    df["following_count"] = result["following_count"]
    df["followers_count"] = result["followers_count"]
    df["tweet_count"] = result["tweet_count"]
    df["listed_count"] = result["listed_count"]

    df.reset_index(inplace=True)

    df = df.reindex(
        [
            "id",
            "text",
            "created_at",
            "retweet_count",
            "reply_count",
            "like_count",
            "quote_count",
            "impression_count",
            "user_profile_clicks",
            "url_link_clicks",
            "following_count",
            "followers_count",
            "tweet_count",
            "listed_count",
        ],
        axis=1,
    )

    # pylint: disable=C0301
    full_filepath = f"/tmp/data/{table_id}/upload_ts={str(int(datetime.now().timestamp()))}/{table_id}.csv"
    folder = full_filepath.replace(table_id + ".csv", "")
    log(folder)
    os.system(f"mkdir -p {folder}")
    df.to_csv(full_filepath, index=False)

    return f"/tmp/data/{table_id}/"


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def crawler_real_time(lst_dimension: list, lst_metric: list, property_id: str) -> str:
    """
    Crawler real time data from Google Analytics API
    """

    ga4 = GA4RealTimeReport(property_id)
    response = ga4.query_report(lst_dimension, lst_metric, 100, True)

    df = pd.DataFrame(response["rows"], columns=response["headers"])

    now = datetime.now().strftime("%Y-%m-%d")

    filepath = f"/tmp/data/date={now}/pageviews.csv"
    partition_path = filepath.replace("pageviews.csv", "")
    os.system(f"mkdir -p {partition_path}")

    df.to_csv(filepath, index=False)

    return "/tmp/data/"


@task(checkpoint=False)
def get_ga_credentials(secret_path: str, key: str, wait=None) -> str:
    """
    Returns the user and password for the given secret path.
    """
    log(f"Getting user and password for secret path: {secret_path}")
    tokens_dict = get_credentials_from_secret(secret_path)
    secret = tokens_dict[key]

    return secret


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def crawler_report_ga(view_id: str, metrics: list = None) -> str:
    """
    Extract data from Google Analytics API for the specified view_id and metrics.
    All metrics are computed by date and merged in a DataFrame.

    Args:
        view_id (str): Google Analytics view_id
        metrics (list): List of metrics to extract from Google Analytics API

    Returns:
        str: Path to the partition folder
    """

    metrics = metrics if metrics else []
    map_report_metric = {}

    analytics = initialize_analyticsreporting()
    for metric in metrics:
        map_report_metric.update(
            {metric: get_report(analytics, "ga:date", f"ga:{metric}", view_id)}
        )

    dfs = []

    for metric in metrics:
        df = parse_data(map_report_metric[metric])
        dfs.append(df)

    df = reduce(lambda left, right: pd.merge(left, right, on="date", how="outer"), dfs)

    df.drop(columns=["date"], inplace=True)

    df.columns = [
        "users_1_day",
        "users_7_days",
        "users_14_days",
        "users_28_days",
        "users_30_days",
        "new_users",
    ]

    now = datetime.now().strftime("%Y-%m-%d")

    filepath = f"/tmp/data/reference_date={now}/users.csv"
    partition_path = filepath.replace("users.csv", "")
    os.system(f"mkdir -p {partition_path}")

    df.to_csv(filepath, index=False)

    return "/tmp/data/"


@task(
    trigger=all_successful,
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def get_data_from_sheet(
    sheet_id: str, sheet_name: str, usecols: Optional[int] = None
) -> pd.DataFrame:
    """Get the data from a Google Sheet, and return a DataFrame."""
    google_sheet = create_google_sheet_url(sheet_id, sheet_name)
    if isinstance(usecols, int):
        usecols = list(range(usecols))
    dff = pd.read_csv(google_sheet, usecols=usecols)
    if "valor" in dff.columns:
        if dff["valor"].dtype == "object":
            dff["valor"] = dff["valor"].str.replace(",", ".")
            dff["valor"] = dff["valor"].astype(float)

    return dff


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def save_data_to_csv(df: pd.DataFrame, filename: str) -> str:
    """Save the DataFrame as csv and return the path to the csv file"""
    out_path = Path("tmp/data")
    if not out_path.is_dir():
        out_path.mkdir(parents=True)
    filepath = f"tmp/data/{filename}.csv"
    df.to_csv(filepath, encoding="utf-8", sep=",", na_rep="", index=False)

    return filepath
