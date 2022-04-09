"""
Tasks for br_twitter
"""
import os
from datetime import datetime, timedelta
from typing import Tuple

import pytz
from prefect import task
import requests
from tqdm import tqdm
from requests_oauthlib import OAuth1
import pandas as pd
import numpy as np

from pipelines.utils.utils import get_storage_blobs, log
from pipelines.datasets.bd_twitter.utils import (
    create_headers,
    create_url,
    connect_to_endpoint,
    flatten,
    get_credentials_from_secret,
)
from pipelines.constants import constants

# pylint: disable=C0103
@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def echo(message: str) -> None:
    '''
    Logs message as a Task
    '''
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
def has_new_tweets(bearer_token: str) -> bool:
    '''
    Checks if there are new tweets to capture data
    '''
    now = datetime.now(tz=pytz.UTC)
    os.system(
        f'mkdir -p /tmp/data/metricas_tweets/upload_day={now.strftime("%Y-%m-%d")}/'
    )

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

    blobs = get_storage_blobs(dataset_id="bd_twitter", table_id="metricas_tweets")
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
    access_secret: str, access_token: str, consumer_key: str, consumer_secret: str
) -> str:
    '''
    Create file with public and non_public_metrics from Twitter API
    '''
    now = datetime.now(tz=pytz.UTC)
    df1 = pd.read_csv(
        "/tmp/basic_metrics.csv",
        dtype={
            "url_link_clicks": int,
            "user_profile_clicks": int,
            "impression_count": int,
        },
    )
    ids = df1["id"].to_list()

    temp_dict = {}
    for id_field in tqdm(ids):
        # retweets don't have non_public_metrics
        if not df1[df1.id == id_field].text.to_list()[0].startswith("RT @"):
            url = f"https://api.twitter.com/2/tweets/{id_field}?tweet.fields=non_public_metrics"

            headeroauth = OAuth1(
                consumer_key,
                consumer_secret,
                access_token,
                access_secret,
                signature_type="auth_header",
            )
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

    # pylint: disable=C0301
    filepath = f'/tmp/data/metricas_tweets/upload_day={now.strftime("%Y-%m-%d")}/metricas_tweets.csv'
    df.to_csv(filepath)

    return filepath


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def crawler_metricas_agg(
    access_secret: str, access_token: str, consumer_key: str, consumer_secret: str
):
    '''
    Task to weekly capture aggregate data from previously created daily twitter data
    '''
    now = datetime.now(tz=pytz.UTC)
    os.system("mkdir -p /tmp/data/metricas_tweets_agg/")

    dfs = []
    blobs = get_storage_blobs(dataset_id="bd_twitter", table_id="metricas_tweets")
    for blob in blobs:
        url_data = blob.public_url
        df = pd.read_csv(url_data, dtype={"id": str}, parse_dates=["created_at"])
        dfs.append(df)

    df = dfs[0].append(dfs[1:])

    df1 = df.groupby("created_at").agg(
        {
            "retweet_count": "sum",
            "reply_count": "sum",
            "like_count": "sum",
            "quote_count": "sum",
            "impression_count": "sum",
            "user_profile_clicks": "sum",
            "url_link_clicks": "sum",
        }
    )

    df1 = df1.reset_index()

    url = (
        "https://api.twitter.com/2/users/1184334528837574656?user.fields=public_metrics"
    )

    headeroauth = OAuth1(
        consumer_key,
        consumer_secret,
        access_token,
        access_secret,
        signature_type="auth_header",
    )
    try:
        r = requests.get(url, auth=headeroauth)
        json_response = r.json()
        result = json_response["data"]["public_metrics"]
    except KeyError:
        log(json_response["errors"])

    df2 = pd.DataFrame(result, index=[1])
    now = datetime.now().strftime("%Y-%m-%d")
    df2["date"] = now

    df1["date"] = [date.strftime("%Y-%m-%d") for date in df1["created_at"]]

    df1 = df1.drop("created_at", axis=1)

    if now not in df1["date"].to_list():
        part = pd.DataFrame([[np.nan] * len(df1.columns)], columns=df1.columns)
        part["date"] = now
        df1 = df1.append(part)

    df = df1.set_index("date").join(df2.set_index("date"))

    filepath = "/tmp/data/metricas_tweets_agg/metricas_tweets_agg.csv"
    df.to_csv(filepath)

    return filepath
