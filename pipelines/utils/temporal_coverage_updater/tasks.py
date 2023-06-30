# -*- coding: utf-8 -*-
"""
Tasks for temporal_coverage_updater
"""


from prefect import task
import basedosdados as bd
from pipelines.utils.temporal_coverage_updater.utils import (
    get_ids,
    parse_temporal_coverage,
    get_date,
    create_update,
)
from datetime import datetime
from pipelines.utils.utils import log, get_credentials_from_secret
from typing import Tuple


@task
def get_credentials(secret_path: str) -> Tuple[str, str]:
    """
    Returns the user and password for the given secret path.
    """
    log(f"Getting user and password for secret path: {secret_path}")
    tokens_dict = get_credentials_from_secret(secret_path)
    email = tokens_dict.get("email")
    password = tokens_dict.get("password")
    return email, password


@task
def find_ids(dataset_id, table_id, email, password):
    """
    Finds the IDs for a given dataset and table.

    Args:
        dataset_id (str): The ID of the dataset.
        table_id (str): The ID of the table.

    Returns:
        dict: A dictionary containing the dataset ID, table ID, and coverage ID.

    Raises:
        Exception: If an error occurs while retrieving the IDs.
    """
    try:
        ids = get_ids(
            dataset_name=dataset_id, table_name=table_id, email=email, password=password
        )
        log(f"IDs >>>> {ids}")
        return ids
    except Exception as e:
        log(f"An error occurred while retrieving the IDs: {str(e)}")
        raise


@task
def extract_last_update(dataset_id, table_id):
    """
    Extracts the last update date of a given dataset table.

    Args:
        dataset_id (str): The ID of the dataset.
        table_id (str): The ID of the table.

    Returns:
        str: The last update date in the format 'YYYY-MM-DD'.

    Raises:
        Exception: If an error occurs while extracting the last update date.
    """
    try:
        query_bd = f"""
        SELECT
        *
        FROM
        `basedosdados-dev.{dataset_id}.__TABLES__`
        WHERE
        table_id = '{table_id}'
        """

        t = bd.read_sql(query=query_bd, billing_project_id="basedosdados")
        timestamp = (
            t["last_modified_time"][0] / 1000
        )  # Convert to seconds by dividing by 1000
        dt = datetime.fromtimestamp(timestamp)
        last_date = dt.strftime("%Y-%m-%d")
        log(f"Última data: {last_date}")
        return last_date
    except Exception as e:
        log(f"An error occurred while extracting the last update date: {str(e)}")
        raise


@task
def get_first_date(ids, email, password):
    """
    Retrieves the first date from the given coverage ID.

    Args:
        ids (dict): A dictionary containing the dataset ID, table ID, and coverage ID.

    Returns:
        str: The first date in the format 'YYYY-MM-DD(interval)'.

    Raises:
        Exception: If an error occurs while retrieving the first date.
    """
    try:
        date = get_date(
            query_class="allDatetimerange",
            query_parameters={"$coverage_Id: ID": ids.get("coverage_id")},
            email=email,
            password=password,
        )
        data = date["data"]["allDatetimerange"]["edges"][0]["node"]
        first_date = f"{data['startYear']}-{data['startMonth']}-{data['startDay']}({data['interval']})"
        log(f"Primeira data: {first_date}")
        return first_date
    except Exception as e:
        log(f"An error occurred while retrieving the first date: {str(e)}")
        raise


@task
def update_temporal_coverage(ids, first_date, last_date, email, password):
    """
    Updates the temporal coverage of a given coverage ID with the specified first and last dates.

    Args:
        ids (dict): A dictionary containing the dataset ID, table ID, and coverage ID.
        first_date (str): The first date in the format 'YYYY-MM-DD'.
        last_date (str): The last date in the format 'YYYY-MM-DD'.

    Raises:
        Exception: If an error occurs while updating the temporal coverage.
    """
    try:
        resource_to_temporal_coverage = parse_temporal_coverage(
            f"{first_date}{last_date}"
        )
        resource_to_temporal_coverage["coverage"] = ids.get("coverage_id")
        log(f"Mutation parameters: {resource_to_temporal_coverage}")
        create_update(
            query_class="allDatetimerange",
            query_parameters={"$coverage_Id: ID": ids.get("coverage_id")},
            mutation_class="CreateUpdateDateTimeRange",
            mutation_parameters=resource_to_temporal_coverage,
            update=True,
            email=email,
            password=password,
        )
    except Exception as e:
        log(f"An error occurred while updating the temporal coverage: {str(e)}")
        raise
