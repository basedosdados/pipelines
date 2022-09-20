# -*- coding: utf-8 -*-
"""
Tasks for br_twitter
"""
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from prefect import task
import requests
import pandas as pd

from pipelines.utils.utils import (
    log,
)
from pipelines.datasets.br_bd_metadados.utils import (
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


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def crawler_organizations():
    """
    Pulls metadata about organizations from Base dos Dados' APIs and returns it structured in a csv file.

    Args:

    Returns:
        Path to csv file with structured metadata about organizations.
    """

    url = "https://basedosdados.org/api/3/action/organization_list"
    r = requests.get(url)
    json_response = r.json()

    organizations_json = json_response["result"]
    organizations = []
    for organization in organizations_json:

        response = requests.get(
            "https://basedosdados.org/api/3/action/organization_show?id={}".format(
                organization
            )
        )
        organization_json = response.json()["result"]

        organizations.append(
            {
                "id": organization_json["id"],
                "name": organization_json["name"],
                "description": organization_json["description"],
                "display_name": organization_json["display_name"],
                "title": organization_json["title"],
                "package_count": organization_json["package_count"],
                "date_created": organization_json["created"][0:10],
            }
        )

    df = pd.DataFrame.from_dict(organizations)

    os.system("mkdir -p /tmp/data/")
    df.to_csv("/tmp/data/organizations.csv", index=False)

    return "/tmp/data/organizations.csv"


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def crawler_datasets():
    """
    Pulls metadata about datasets from Base dos Dados' APIs and returns it structured in a csv file.

    Args:

    Returns:
        Path to csv file with structured metadata about datasets.
    """

    url = "https://basedosdados.org/api/3/action/bd_dataset_search?page_size=50000"
    r = requests.get(url)
    datasets_json = r.json()["result"]["datasets"]

    datasets = []
    for dataset in datasets_json:

        datasets.append(
            {
                "organization_id": dataset["owner_org"],
                "id": dataset["id"],
                "name": dataset["name"],
                "title": dataset["title"],
                "date_created": dataset["metadata_created"][0:10],
                "date_last_modified": dataset["metadata_modified"][0:10],
                "themes": [group["id"] for group in dataset["groups"]],
                "tags": [tag["id"] for tag in dataset["tags"]],
            }
        )

    df = pd.DataFrame.from_dict(datasets)

    os.system("mkdir -p /tmp/data/")
    df.to_csv("/tmp/data/datasets.csv", index=False)

    return "/tmp/data/datasets.csv"


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def crawler_resources():
    """
    Pulls metadata about resources from Base dos Dados' APIs and returns it structured in a csv file.

    Args:

    Returns:
        Path to csv file with structured metadata about resources.
    """

    url = "https://basedosdados.org/api/3/action/bd_dataset_search?page_size=50000"
    r = requests.get(url)
    datasets_json = r.json()["result"]["datasets"]

    resources = []
    for dataset in datasets_json:
        for resource in dataset["resources"]:
            if resource.get("created") is not None:

                resources.append(
                    {
                        "dataset_id": dataset["id"],
                        "id": resource["id"],
                        "name": resource["name"],
                        "date_created": resource["created"][0:10],
                        "date_last_modified": resource["metadata_modified"][0:10],
                        "type": resource["resource_type"],
                    }
                )

    df = pd.DataFrame.from_dict(resources)

    os.system("mkdir -p /tmp/data/")
    df.to_csv("/tmp/data/resources.csv", index=False)

    return "/tmp/data/resources.csv"


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def crawler_external_links():
    """
    Pulls metadata about external links from Base dos Dados' APIs and returns it structured in a csv file.

    Args:

    Returns:
        Path to csv file with structured metadata about external links.
    """

    url = "https://basedosdados.org/api/3/action/bd_dataset_search?page_size=50000&resource_type=external_link"
    r = requests.get(url)
    datasets_json = r.json()["result"]["datasets"]

    resources = []
    for dataset in datasets_json:
        for resource in dataset["resources"]:
            if resource["resource_type"] == "external_link":

                resources.append(
                    {
                        "dataset_id": dataset.get("id"),
                        "id": resource.get("id"),
                        "name": resource.get("name"),
                        "date_created": resource.get("created")[0:10],
                        "date_last_modified": resource.get("metadata_modified")[0:10],
                        "url": resource.get("url"),
                        "language": resource.get("language"),
                        "has_structured_data": resource.get("has_structured_data"),
                        "has_api": resource.get("has_api"),
                        "is_free": resource.get("is_free"),
                        "requires_registration": resource.get("requires_registration"),
                        "availability": resource.get("availability"),
                        "spatial_coverage": resource.get("spatial_coverage"),
                        "temporal_coverage": resource.get("temporal_coverage"),
                        "update_frequency": resource.get("update_frequency"),
                        "observation_level": resource.get("observation_level"),
                    }
                )

    df = pd.DataFrame.from_dict(resources)

    os.system("mkdir -p /tmp/data/")
    df.to_csv("/tmp/data/external_links.csv", index=False)

    return "/tmp/data/external_links.csv"


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def crawler_information_requests():
    """
    Pulls metadata about information requests from Base dos Dados' APIs and returns it structured in a csv file.

    Args:

    Returns:
        Path to csv file with structured metadata about information requests.
    """

    url = "https://basedosdados.org/api/3/action/bd_dataset_search?page_size=50000&resource_type=information_request"
    r = requests.get(url)
    datasets_json = r.json()["result"]["datasets"]

    resources = []
    for dataset in datasets_json:
        for resource in dataset["resources"]:
            if resource["resource_type"] == "information_request":

                resources.append(
                    {
                        "dataset_id": dataset.get("id"),
                        "id": resource.get("id"),
                        "name": resource.get("name"),
                        "date_created": resource.get("created")[0:10],
                        "date_last_modified": resource.get("metadata_modified")[0:10],
                        "url": resource.get("url"),
                        "origin": resource.get("origin"),
                        "number": resource.get("number"),
                        "opening_date": resource.get("opening_date"),
                        "requested_by": resource.get("requested_by"),
                        "status": resource.get("status"),
                        "data_url": resource.get("data_url"),
                        "spatial_coverage": resource.get("spatial_coverage"),
                        "temporal_coverage": resource.get("temporal_coverage"),
                        "update_frequency": resource.get("update_frequency"),
                        "observation_level": resource.get("observation_level"),
                    }
                )

    df = pd.DataFrame.from_dict(resources)

    os.system("mkdir -p /tmp/data/")
    df.to_csv("/tmp/data/information_requests.csv", index=False)

    return "/tmp/data/information_requests.csv"


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def crawler_tables():
    """
    Pulls metadata about tables from Base dos Dados' APIs and returns it structured in a csv file.

    Args:

    Returns:
        Path to csv file with structured metadata about tables.
    """

    url = "https://basedosdados.org/api/3/action/bd_dataset_search?page_size=50000&resource_type=bdm_table"
    r = requests.get(url)
    json_response = r.json()

    current_date = datetime.today()
    datasets = json_response["result"]["datasets"]
    resources = []
    for dataset in datasets:
        for resource in dataset["resources"]:
            if resource["resource_type"] == "bdm_table":

                if "created" in resource:
                    if resource["created"] is not None:
                        date_created = resource["created"][0:10]
                else:
                    date_created = None

                if "metadata_modified" in resource:
                    if resource["metadata_modified"] is not None:
                        date_last_modified = resource["metadata_modified"][0:10]
                else:
                    date_last_modified = None

                if "number_rows" in resource:
                    if resource["number_rows"] is not None:
                        number_rows = int(resource["number_rows"])
                else:
                    number_rows = 0

                # indicator for outdated
                outdated = None
                if "update_frequency" in resource:
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
                                outdated = 1
                            else:
                                outdated = 0

                resources.append(
                    {
                        "dataset_id": dataset.get("id"),
                        "id": resource.get("id"),
                        "table_id": resource.get("name"),
                        "date_created": date_created,
                        "date_last_modified": date_last_modified,
                        "spatial_coverage": resource.get("spatial_coverage"),
                        "temporal_coverage": resource.get("temporal_coverage"),
                        "update_frequency": resource.get("update_frequency"),
                        "observation_level": resource.get("observation_level"),
                        "number_rows": number_rows,
                        "number_columns": len(resource.get("columns")),
                        "outdated": outdated,
                    }
                )

    df = pd.DataFrame.from_dict(resources)

    os.system("mkdir -p /tmp/data/")
    df.to_csv("/tmp/data/tables.csv", index=False)

    return "/tmp/data/tables.csv"


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def crawler_columns():
    """
    Pulls metadata about columns from Base dos Dados' APIs and returns it structured in a csv file.

    Args:

    Returns:
        Path to csv file with structured metadata about columns.
    """

    url = "https://basedosdados.org/api/3/action/bd_dataset_search?page_size=50000&resource_type=bdm_table"
    r = requests.get(url)
    json_response = r.json()

    datasets = json_response["result"]["datasets"]
    columns = []
    for dataset in datasets:
        for resource in dataset["resources"]:
            if resource["resource_type"] == "bdm_table":
                for column in resource.get("columns"):

                    directory_column = None
                    if column.get("directory_column") is not None:
                        if column.get("directory_column")["dataset_id"] is not None:
                            directory_column = column.get("directory_column")

                    columns.append(
                        {
                            "table_id": resource.get("id"),
                            "name": column.get("name"),
                            "bigquery_type": column.get("bigquery_type"),
                            "description": column.get("description"),
                            "temporal_coverage": column.get("temporal_coverage"),
                            "covered_by_dictionary": column.get(
                                "covered_by_dictionary"
                            ),
                            "directory_column": directory_column,
                            "measurement_unit": column.get("measurement_unit"),
                            "has_sensitive_data": column.get("has_sensitive_data"),
                            "observations": column.get("observations"),
                            "is_in_staging": column.get("is_in_staging"),
                            "is_partition": column.get("is_partition"),
                        }
                    )

    df = pd.DataFrame.from_dict(columns)

    os.system("mkdir -p /tmp/data/")
    df.to_csv("/tmp/data/columns.csv", index=False)

    return "/tmp/data/columns.csv"
