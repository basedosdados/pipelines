# -*- coding: utf-8 -*-
"""
utils for br_twitter
"""
import collections
from typing import Tuple

import requests


def create_headers(bearer_token: str) -> dict:
    """
    Create header to use in endpoint connection
    """
    headers = {"Authorization": f"Bearer {bearer_token}"}
    return headers


def create_url(start_date: str, end_date: str, max_results=10) -> Tuple[str, dict]:
    """
    Creates parameterized url
    """
    ttid = 1184334528837574656
    # pylint: disable=C0301
    search_url = f"https://api.twitter.com/2/users/{ttid}/tweets"  # Change to the endpoint you want to collect data from

    # change params based on the endpoint you are using
    query_params = {
        "start_time": start_date,
        "end_time": end_date,
        "max_results": max_results,
        "tweet.fields": "public_metrics,created_at",
        "next_token": {},
    }

    return (search_url, query_params)


def connect_to_endpoint(url: str, headers: dict, params: dict, next_token=None) -> dict:
    """
    Connect to endpoint using params
    """
    params["next_token"] = next_token  # params object received from create_url function
    response = requests.request("GET", url, headers=headers, params=params)
    print("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


# pylint: disable=C0103
def flatten(d: dict, parent_key="", sep="_") -> dict:
    """
    Flatten a dict recursively
    """
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)
