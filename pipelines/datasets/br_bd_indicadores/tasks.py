# -*- coding: utf-8 -*-
"""
Tasks for br_twitter
"""
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import Tuple
from functools import reduce

import pytz
from prefect import task
import requests
from tqdm import tqdm
from requests_oauthlib import OAuth1
import pandas as pd
import numpy as np

from pipelines.utils.utils import (
    get_storage_blobs,
    log,
    get_credentials_from_secret,
    add_underscore_to_column_name,
)
from pipelines.datasets.br_bd_indicadores.utils import (
    create_headers,
    create_url,
    connect_to_endpoint,
    flatten,
    GA4RealTimeReport,
    parse_data,
    initialize_analyticsreporting,
    get_report,
    get_temporal_coverage_list,
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
                r = requests.get(url, auth=headeroauth)

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

    url = (
        "https://api.twitter.com/2/users/1184334528837574656?user.fields=public_metrics"
    )
    try:
        r = requests.get(url, auth=headeroauth)
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

    df = add_underscore_to_column_name(df)

    now = datetime.now().strftime("%Y-%m-%d")

    filepath = f"/tmp/data/upload_day={now}/users.csv"
    partition_path = filepath.replace("users.csv", "")
    os.system(f"mkdir -p {partition_path}")

    df.to_csv(filepath, index=False)

    return "/tmp/data/"


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def crawler_data_quality():
    """

    Args:

    Returns:
    """

    url = "https://basedosdados.org/api/3/action/bd_dataset_search?page_size=10000"
    r = requests.get(url)
    json_response = r.json()

    current_date = datetime.today()
    datasets = json_response["result"]["datasets"]
    tables = []
    for dataset in datasets:
        for resource in dataset["resources"]:
            if resource["resource_type"] == "bdm_table":
                if "update_frequency" in resource:

                    # indicator for outdated
                    outdated = None
                    if resource["update_frequency"] in [
                        "recurring",
                        "unique",
                        "uncertain",
                        "other",
                        None,
                    ]:
                        pass
                    else:
                        if resource["temporal_coverage"] in [None, []]:
                            pass
                        else:
                            upper_temporal_coverage = get_temporal_coverage_list(
                                resource["temporal_coverage"]
                            )[-1]
                            if resource["update_frequency"] in [
                                "one_year",
                                "semester",
                                "quarter",
                                "month",
                                "week",
                                "day",
                                "hour",
                                "minute",
                                "second",
                            ]:
                                delta = 1
                            elif resource["update_frequency"] == "two_years":
                                delta = 2
                            elif resource["update_frequency"] == "four_years":
                                delta = 4
                            elif resource["update_frequency"] == "five_years":
                                delta = 5
                            elif resource["update_frequency"] == "ten_years":
                                delta = 10
                            else:
                                delta = 1000

                            number_dashes = upper_temporal_coverage.count("-")
                            if number_dashes == 0:
                                upper_temporal_coverage = datetime.strptime(
                                    upper_temporal_coverage, "%Y"
                                )
                                diff = relativedelta(years=delta)
                            elif number_dashes == 1:
                                upper_temporal_coverage = datetime.strptime(
                                    upper_temporal_coverage, "%Y-%m"
                                )
                                diff = relativedelta(month=delta)
                            elif number_dashes == 2:
                                upper_temporal_coverage = datetime.strptime(
                                    upper_temporal_coverage, "%Y-%m-%d"
                                )
                                diff = relativedelta(days=delta)

                            if current_date > upper_temporal_coverage + diff:
                                outdated = True
                            else:
                                outdated = False

                    tables.append(
                        {
                            "resource_id": resource["id"],
                            "dataset_id": dataset["name"],
                            "table_id": resource["name"],
                            "temporal_coverage": resource["temporal_coverage"],
                            "update_frequency": resource["update_frequency"],
                            "outdated": outdated,
                        }
                    )

    df = pd.DataFrame.from_dict(tables)

    os.system("mkdir -p /tmp/data/")
    df.to_csv("/tmp/data/data_quality.csv", index=False)

    return "/tmp/data/data_quality.csv"
