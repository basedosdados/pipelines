# -*- coding: utf-8 -*-
"""
utils for br_bd_inndicadores
"""
# pylint: disable=too-few-public-methods
import collections
from typing import Tuple
from typing import List
import os
from datetime import datetime

from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    Dimension,
    Metric,
)
from google.analytics.data_v1beta.types import (
    RunRealtimeReportRequest,
)
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
    response = requests.request("GET", url, headers=headers, params=params, timeout=30)
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
    """base class for GA4 exceptions"""


class GA4RealTimeReport:
    """class to query GA4 real time report
    More information: https://support.google.com/analytics/answer/9271392?hl=en
    """

    def __init__(self, property_id):
        self.property_id = property_id
        self.client = BetaAnalyticsDataClient()

    def query_report(
        self,
        dimensions: List[str],
        metrics: List[Metric],
        row_limit: int = 10000,
        quota_usage: bool = False,
    ):
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
                property=f"properties/{self.property_id}",
                dimensions=dimension_list,
                metrics=metrics_list,
                limit=row_limit,
                return_property_quota=quota_usage,
            )
            response = self.client.run_realtime_report(report_request)

            output = {}
            if "property_quota" in response:
                output["quota"] = response.property_quota

            # construct the dataset
            headers = [header.name for header in response.dimension_headers] + [
                header.name for header in response.metric_headers
            ]
            rows = []
            for row in response.rows:
                rows.append(
                    [dimension_value.value for dimension_value in row.dimension_values]
                    + [metric_value.value for metric_value in row.metric_values]
                )
            output["headers"] = headers
            output["rows"] = rows
            return output
        except Exception as e:
            raise GA4Exception(e) from e


def initialize_analyticsreporting():
    """Initializes an Analytics Reporting API V4 service object.
    Returns:
      An authorized Analytics Reporting API V4 service object.
    """
    KEY_FILE_LOCATION = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        KEY_FILE_LOCATION, SCOPES
    )

    # Build the service object.
    analytics = build("analyticsreporting", "v4", credentials=credentials)

    return analytics


def get_report(analytics, dimension: str, metric: str, VIEW_ID: str):
    """Queries the Analytics Reporting API V4.
    Args:
        analytics: An authorized Analytics Reporting API V4 service object.
        dimension: The name of the dimension to query.
        metric: The name of the metric to query.
        VIEW_ID: The ID of the view to retrieve data for.
    Returns:
        The Analytics Reporting API V4 response.
    """
    return (
        analytics.reports()
        .batchGet(
            body={
                "reportRequests": [
                    {
                        "viewId": VIEW_ID,
                        "dateRanges": [{"startDate": "today", "endDate": "today"}],
                        "metrics": [{"expression": metric}],
                        "dimensions": [{"name": dimension}],
                    }
                ]
            }
        )
        .execute()
    )


def parse_data(response) -> pd.DataFrame:
    """Parses the Analytics Reporting API V4 response.

    Args:
        response: An Analytics Reporting API V4 response.

    Returns:
        Dataframe with the response data.
    """
    reports = response["reports"][0]
    columnHeader = reports["columnHeader"]["dimensions"]
    metricHeader = reports["columnHeader"]["metricHeader"]["metricHeaderEntries"]
    # Get dimenssion names
    dim_names = [columnHeader[n].split(":")[1] for n in range(len(columnHeader))]
    # Get metric names
    metric_names = [
        metricHeader[n]["name"].split(":")[1] for n in range(len(metricHeader))
    ]
    column_names = dim_names + metric_names

    columns = columnHeader
    for metric in metricHeader:
        columns.append(metric["name"])

    data = pd.json_normalize(reports["data"]["rows"])
    data_dimensions = pd.DataFrame(data["dimensions"].tolist())
    data_metrics = pd.DataFrame(data["metrics"].tolist())
    data_metrics = data_metrics.applymap(lambda x: x["values"])
    data_metrics = pd.DataFrame(data_metrics[0].tolist())
    result = pd.concat([data_dimensions, data_metrics], axis=1, ignore_index=True)

    # Assign columns names to DF
    result.columns = column_names
    return result


def flatten_list(ll):
    """Flatten a list of lists"""
    return [str(item) for sublist in ll for item in sublist]


def classify_frequency(interval):
    """
    Classify a temporal coverage interval's frequency as: year, month, day, other
    """

    if "(" in interval and ")" in interval:
        initial = interval.split("(")[0]
        final = interval.split(")")[1]
    else:
        initial = interval
        final = interval

    initial_dashes = initial.count("-")
    final_dashes = final.count("-")

    if max(initial_dashes, final_dashes) == 2:
        frequency = "day"
    elif max(initial_dashes, final_dashes) == 1:
        frequency = "month"
    elif max(initial_dashes, final_dashes) == 0:
        frequency = "year"
    else:
        frequency = "other"

    return frequency


def get_temporal_coverage_elements(interval):
    """
    Return the three temporal coverage interval's elements: start, end, delta
    """

    if "(" in interval and ")" in interval:
        start = interval.split("(")[0]
        end = interval.split(")")[1]
        delta = int(interval[interval.find("(") + 1 : interval.find(")")])
    else:
        start = interval
        end = interval
        delta = 0

    return start, end, delta


def get_interval_range(start, end, delta, frequency):
    """
    Return a range list based on an interval's elements
    """

    if delta > 0:
        if frequency == "year":
            sub = [*range(int(start), int(end) + 1, delta)]
        elif frequency == "month":
            start_date = datetime.strptime(start, "%Y-%m")
            end_date = datetime.strptime(end, "%Y-%m")
            date_range = pd.period_range(start_date, end_date, freq="M")
            sub = [str(date) for date in date_range]
        elif frequency == "day":
            start_date = datetime.strptime(start, "%Y-%m-%d")
            end_date = datetime.strptime(end, "%Y-%m-%d")
            date_range = pd.period_range(start_date, end_date, freq="D")
            sub = [str(date) for date in date_range]
        else:
            pass
    else:
        sub = [start]

    return sub


def get_temporal_coverage_list(temporal_coverage_field: list) -> list:
    """
    Generates a list of dates covered by resource in format YYYY[-MM][-DD].
    """

    temporal_coverage_list = []

    for s in temporal_coverage_field:

        frequency = classify_frequency(s)
        start, end, delta = get_temporal_coverage_elements(s)
        sub = get_interval_range(start, end, delta, frequency)
        temporal_coverage_list.append(sub)

    return flatten_list(temporal_coverage_list)
