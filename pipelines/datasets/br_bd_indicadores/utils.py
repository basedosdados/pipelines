# -*- coding: utf-8 -*-
"""
utils for br_bd_inndicadores
"""

from typing import List
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (Dimension, Metric, DateRange, Metric, OrderBy, 
                                               FilterExpression, MetricAggregation, CohortSpec)
from google.analytics.data_v1beta.types import RunReportRequest, RunRealtimeReportRequest
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



class GA4Exception(Exception):
    '''base class for GA4 exceptions'''

class GA4RealTimeReport:
    """class to query GA4 real time report
    More information: https://support.google.com/analytics/answer/9271392?hl=en
    """

    def __init__(self, property_id):
        self.property_id = property_id
        self.client = BetaAnalyticsDataClient()

    def query_report(self, dimensions: List[str], metrics: List[Metric], row_limit:int=10000, quota_usage:bool=False):
        """
        :param dimensions: categorical attributes (age, country, city, etc)
        :type dimensions: [dimension type]
                :param dimensions: categorical attributes (age, country, city, etc)

        :param metrics: numeric attributes (views, user count, active users)
        :type metrics: [metric type]

        """
        try:
            dimension_list = [Dimension(name=dim) for dim in dimensions]
            metrics_list = [Metric(name=m) for m in metrics]
            
            report_request = RunRealtimeReportRequest(
                property=f'properties/{self.property_id}',
                dimensions=dimension_list,
                metrics=metrics_list,
                limit=row_limit,
                return_property_quota=quota_usage
            )
            response = self.client.run_realtime_report(report_request)
     
            output = {}
            if 'property_quota' in response:
                output['quota'] = response.property_quota

            # construct the dataset
            headers = [header.name for header in response.dimension_headers] + [header.name for header in response.metric_headers]
            rows = []
            for row in response.rows:
                rows.append(
                    [dimension_value.value for dimension_value in row.dimension_values] + \
                    [metric_value.value for metric_value in row.metric_values])            
            output['headers'] = headers
            output['rows'] = rows
            return output
        except Exception as e:
            raise GA4Exception(e)

