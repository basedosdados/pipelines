# -*- coding: utf-8 -*-
"""
General purpose functions for the metadata project
"""

import json
import re

# pylint: disable=too-many-arguments
from datetime import datetime
from typing import Tuple

import basedosdados as bd
import numpy as np
import pandas as pd
import requests

from pipelines.utils.utils import get_credentials_from_secret, log


#######################
# Django Metadata Utils
#######################
def get_first_date(
    ids,
    email,
    password,
    date_format: str,
    api_mode: str = "prod",
):
    """
    Retrieves the first date from the given coverage ID.

    Args:
        ids (dict): A dictionary containing the dataset ID, table ID, and coverage ID.
        date_format (str): Date format ('yy-mm-dd', 'yy-mm', or 'yy')
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
            api_mode=api_mode,
        )
        data = date["data"]["allDatetimerange"]["edges"][0]["node"]
        log(data)
        if date_format == "yy-mm-dd":
            first_date = f"{data['startYear']}-{data['startMonth']}-{data['startDay']}({data['interval']})"
        elif date_format == "yy-mm":
            first_date = f"{data['startYear']}-{data['startMonth']}({data['interval']})"
        elif date_format == "yy":
            first_date = f"{data['startYear']}({data['interval']})"

        log(f"Primeira data: {first_date}")
        return first_date
    except Exception as e:
        log(f"An error occurred while retrieving the first date: {str(e)}")
        raise


def extract_last_update(
    dataset_id, table_id, date_format: str, billing_project_id: str
):
    """
    Extracts the last update date of a given dataset table.

    Args:
        dataset_id (str): The ID of the dataset.
        table_id (str): The ID of the table.
        date_format (str): Date format ('yy-mm-dd', 'yy-mm', or 'yy')

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
        `basedosdados.{dataset_id}.__TABLES__`
        WHERE
        table_id = '{table_id}'
        """

        t = bd.read_sql(
            query=query_bd,
            billing_project_id=billing_project_id,
            from_file=True,
        )
        timestamp = (
            t["last_modified_time"][0] / 1000
        )  # Convert to seconds by dividing by 1000
        dt = datetime.fromtimestamp(timestamp)
        if date_format == "yy-mm-dd":
            last_date = dt.strftime("%Y-%m-%d")
        elif date_format == "yy-mm":
            last_date = dt.strftime("%Y-%m")
        elif date_format == "yy":
            last_date = dt.strftime("%Y")
        log(f"Última data: {last_date}")
        return last_date
    except Exception as e:
        log(f"An error occurred while extracting the last update date: {str(e)}")
        raise


def extract_last_date(dataset_id, table_id, date_format: str, billing_project_id: str):
    """
    Extracts the last update date of a given dataset table.

    Args:
        dataset_id (str): The ID of the dataset.
        table_id (str): The ID of the table.
        date_format (str): Date format ('yy-mm' or 'yy-mm-dd')
        if set to 'yy-mm' the function will look for  ano and mes named columns in the table_id
        and return a concatenated string in the formar yyyy-mm. if set to 'yyyy-mm-dd'
        the function will look for  data named column in the format 'yyyy-mm-dd' and return it.

    Returns:
        str: The last update date in the format 'yyyy-mm' or 'yyyy-mm-dd'.

    Raises:
        Exception: If an error occurs while extracting the last update date.
    """
    if date_format == "yy-mm":
        try:
            query_bd = f"""
            SELECT
            MAX(CONCAT(ano,"-",mes)) as max_date
            FROM
            `{billing_project_id}.{dataset_id}.{table_id}`
            """

            t = bd.read_sql(
                query=query_bd,
                billing_project_id=billing_project_id,
                from_file=True,
            )
            input_date_str = t["max_date"][0]

            date_obj = datetime.strptime(input_date_str, "%Y-%m")

            last_date = date_obj.strftime("%Y-%m")
            log(f"Última data YYYY-MM: {last_date}")

            return last_date
        except Exception as e:
            log(f"An error occurred while extracting the last update date: {str(e)}")
            raise
    else:
        try:
            query_bd = f"""
            SELECT
            MAX(data) as max_date
            FROM
            `basedosdados.{dataset_id}.{table_id}`
            """
            log(f"Query: {query_bd}")
            t = bd.read_sql(
                query=query_bd,
                billing_project_id=billing_project_id,
                from_file=True,
            )
            # it infers that the data variable is already on basedosdados standart format
            # yyyy-mm-dd
            last_date = t["max_date"][0]

            log(f"Última data YYYY-MM-DD: {last_date}")

            return last_date
        except Exception as e:
            log(f"An error occurred while extracting the last update date: {str(e)}")
            raise


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


def get_credentials_utils(secret_path: str) -> Tuple[str, str]:
    """
    Returns the user and password for the given secret path.
    """
    log(f"Getting user and password for secret path: {secret_path}")
    tokens_dict = get_credentials_from_secret(secret_path)
    email = tokens_dict.get("email")
    password = tokens_dict.get("password")
    return email, password


def get_token(email, password, api_mode: str = "prod"):
    """
    Get api token.
    """
    r = None
    
    if api_mode == "prod":
        url = "http://api.basedosdados.org/api/v1/graphql"
    elif api_mode == "staging":
        url = "http://staging.api.basedosdados.org/api/v1/graphql"

    r = requests.post(
        url = url,
        headers={"Content-Type": "application/json"},
        json={
            "query": """
        mutation ($email: String!, $password: String!) {
            tokenAuth(email: $email, password: $password) {
                token
            }
        }
    """,
            "variables": {"email": email, "password": password},
        },
    )
    r.raise_for_status()

    return r.json()["data"]["tokenAuth"]["token"]


def get_id(
    query_class,
    query_parameters,
    email,
    password,
    api_mode: str = "prod",
    cloud_table: bool = True,
):
    token = get_token(email, password, api_mode)
    header = {
        "Authorization": f"Bearer {token}",
    }
    _filter = ", ".join(list(query_parameters.keys()))

    keys = [
        parameter.replace("$", "").split(":")[0]
        for parameter in list(query_parameters.keys())
    ]

    values = list(query_parameters.values())

    _input = ", ".join([f"{key}:${key}" for key in keys])

    if cloud_table:
        query = f"""query({_filter}) {{
                            {query_class}({_input}){{
                            edges{{
                                node{{
                                id,
                                table{{
                                    _id
                                    }}
                                }}
                            }}
                            }}
                        }}"""

    else:
        query = f"""query({_filter}) {{
                            {query_class}({_input}){{
                            edges{{
                                node{{
                                id,
                                }}
                            }}
                            }}
                        }}"""

    if api_mode == "staging":
        url=f"https://staging.api.basedosdados.org/api/v1/graphql",
    elif api_mode == "prod":
        url="https://api.basedosdados.org/api/v1/graphql"

    r = requests.post(
        url=url,
        json={"query": query, "variables": dict(zip(keys, values))},
        headers=header,
    ).json()

    if "data" in r and r is not None:
        if r.get("data", {}).get(query_class, {}).get("edges") == []:
            id = None
            # print(f"get: not found {query_class}", dict(zip(keys, values)))
        else:
            id = r["data"][query_class]["edges"][0]["node"]["id"]
            # print(f"get: found {id}")
            id = id.split(":")[1]

        return r, id
    else:
        print("get:  Error:", json.dumps(r, indent=4, ensure_ascii=False))
        raise Exception("get: Error")


def get_date(
    query_class,
    query_parameters,
    email,
    password,
    api_mode: str = "prod",
):
    token = get_token(
        email=email,
        password=password,
        api_mode=api_mode,
    )
    log("puxou token")
    header = {
        "Authorization": f"Bearer {token}",
    }
    log(f"{header}")
    _filter = ", ".join(list(query_parameters.keys()))
    keys = [
        parameter.replace("$", "").split(":")[0]
        for parameter in list(query_parameters.keys())
    ]
    values = list(query_parameters.values())
    _input = ", ".join([f"{key}:${key}" for key in keys])

    query = f"""query({_filter}) {{
                        {query_class}({_input}){{
                        edges{{
                            node{{
                            startYear,
                            startMonth,
                            startDay,
                            interval,
                            }}
                        }}
                        }}
                    }}"""

    if api_mode == "staging":
        r = requests.post(
            url=f"https://{api_mode}.api.basedosdados.org/api/v1/graphql",
            json={"query": query, "variables": dict(zip(keys, values))},
            headers=header,
        ).json()
    elif api_mode == "prod":
        r = requests.post(
            url="https://api.basedosdados.org/api/v1/graphql",
            json={"query": query, "variables": dict(zip(keys, values))},
            headers=header,
        ).json()

    return r


def create_update(
    email,
    password,
    mutation_class,
    mutation_parameters,
    query_class,
    query_parameters,
    update=False,
    api_mode: str = "prod",
):
    token = get_token(
        email=email,
        password=password,
        api_mode=api_mode,
    )
    header = {
        "Authorization": f"Bearer {token}",
    }

    r, id = get_id(
        query_class=query_class,
        query_parameters=query_parameters,
        email=email,
        password=password,
        cloud_table=False,
        api_mode=api_mode,
    )
    if id is not None:
        r["r"] = "query"
        if update is False:
            return r, id

    _classe = mutation_class.replace("CreateUpdate", "").lower()
    query = f"""
                mutation($input:{mutation_class}Input!){{
                    {mutation_class}(input: $input){{
                    errors {{
                        field,
                        messages
                    }},
                    clientMutationId,
                    {_classe} {{
                        id,
                    }}
                }}
                }}
            """

    if update is True and id is not None:
        mutation_parameters["id"] = id

        if api_mode == "prod":
            r = requests.post(
                "https://api.basedosdados.org/api/v1/graphql",
                json={"query": query, "variables": {"input": mutation_parameters}},
                headers=header,
            ).json()
        elif api_mode == "staging":
            r = requests.post(
                f"https://{api_mode}api.basedosdados.org/api/v1/graphql",
                json={"query": query, "variables": {"input": mutation_parameters}},
                headers=header,
            ).json()

    r["r"] = "mutation"
    if "data" in r and r is not None:
        if r.get("data", {}).get(mutation_class, {}).get("errors", []) != []:
            print(f"create: not found {mutation_class}", mutation_parameters)
            print("create: error\n", json.dumps(r, indent=4, ensure_ascii=False), "\n")
            id = None
            raise Exception("create: Error")
        else:
            id = r["data"][mutation_class][_classe]["id"]
            id = id.split(":")[1]

            return r, id
    else:
        print("\n", "create: query\n", query, "\n")
        print(
            "create: input\n",
            json.dumps(mutation_parameters, indent=4, ensure_ascii=False),
            "\n",
        )
        print("create: error\n", json.dumps(r, indent=4, ensure_ascii=False), "\n")
        raise Exception("create: Error")


def parse_temporal_coverage(temporal_coverage):
    # Extrai as informações de data e intervalo da string
    if "(" in temporal_coverage:
        start_str, interval_str, end_str = re.split(r"[(|)]", temporal_coverage)
        if start_str == "" and end_str != "":
            start_str = end_str
        elif end_str == "" and start_str != "":
            end_str = start_str
    elif len(temporal_coverage) >= 4:
        start_str, interval_str, end_str = temporal_coverage, 1, temporal_coverage
    start_len = 0 if start_str == "" else len(start_str.split("-"))
    end_len = 0 if end_str == "" else len(end_str.split("-"))

    def parse_date(position, date_str, date_len):
        result = {}
        if date_len == 3:
            date = datetime.strptime(date_str, "%Y-%m-%d")
            result[f"{position}Year"] = date.year
            result[f"{position}Month"] = date.month
            result[f"{position}Day"] = date.day
        elif date_len == 2:
            date = datetime.strptime(date_str, "%Y-%m")
            result[f"{position}Year"] = date.year
            result[f"{position}Month"] = date.month
        elif date_len == 1:
            date = datetime.strptime(date_str, "%Y")
            result[f"{position}Year"] = date.year
        return result

    start_result = parse_date(position="start", date_str=start_str, date_len=start_len)
    end_result = parse_date(position="end", date_str=end_str, date_len=end_len)
    start_result.update(end_result)

    if interval_str != 0:
        start_result["interval"] = int(interval_str)

    return end_result


def get_ids(
    dataset_name: str,
    table_name: str,
    email: str,
    is_bd_pro: bool,
    password: str,
    is_free: bool,
    api_mode: str,
) -> dict:
    """
    Obtains the IDs of the table and coverage based on the provided names.
    """
    try:
        # Get the table ID
        table_result = get_id(
            email=email,
            password=password,
            query_class="allCloudtable",
            query_parameters={
                "$gcpDatasetId: String": dataset_name,
                "$gcpTableId: String": table_name,
            },
            cloud_table=True,
            api_mode=api_mode,
        )
        if not table_result:
            raise ValueError("Table ID not found.")

        table_id = table_result[0]["data"]["allCloudtable"]["edges"][0]["node"][
            "table"
        ].get("_id")
        log(table_id)
        if is_bd_pro and is_free:
            # Get the coverage IDs
            coverage_result = get_id(
                email=email,
                password=password,
                query_class="allCoverage",
                query_parameters={
                    "$table_Id: ID": table_id,
                    "$isClosed: Boolean": True,
                },
                api_mode=api_mode,
                cloud_table=True,
            )
            log(f"----- coverage result {coverage_result}")
            if not coverage_result:
                raise ValueError("Coverage ID not found.")

            coverage_ids = coverage_result[0]

            # Check if there are multiple coverage IDs
            if len(coverage_ids["data"]["allCoverage"]["edges"]) > 1:
                print(
                    "WARNING: Your table has more than one coverage. Only the first ID has been selected."
                )

            # Retrieve the first coverage ID
            coverage_id_pro = coverage_ids["data"]["allCoverage"]["edges"][0]["node"][
                "id"
            ].split(":")[-1]
            # Get the coverage IDs
            coverage_result = get_id(
                email=email,
                password=password,
                query_class="allCoverage",
                query_parameters={
                    "$table_Id: ID": table_id,
                    "$isClosed: Boolean": False,
                },
                api_mode=api_mode,
                cloud_table=True,
            )
            if not coverage_result:
                raise ValueError("Coverage ID not found.")

            coverage_ids = coverage_result[0]

            # Check if there are multiple coverage IDs
            if len(coverage_ids["data"]["allCoverage"]["edges"]) > 1:
                print(
                    "WARNING: Your table has more than one coverage. Only the first ID has been selected."
                )

            # Retrieve the first coverage ID
            coverage_id = coverage_ids["data"]["allCoverage"]["edges"][0]["node"][
                "id"
            ].split(":")[-1]
        elif is_bd_pro and not is_free:
            # Get the coverage IDs
            coverage_result = get_id(
                email=email,
                password=password,
                query_class="allCoverage",
                query_parameters={
                    "$table_Id: ID": table_id,
                    "$isClosed: Boolean": True,
                },
                api_mode=api_mode,
                cloud_table=True,
            )
            if not coverage_result:
                raise ValueError("Coverage ID not found.")

            coverage_ids = coverage_result[0]

            # Check if there are multiple coverage IDs
            if len(coverage_ids["data"]["allCoverage"]["edges"]) > 1:
                print(
                    "WARNING: Your table has more than one coverage. Only the first ID has been selected."
                )

            # Retrieve the first coverage ID
            coverage_id_pro = coverage_ids["data"]["allCoverage"]["edges"][0]["node"][
                "id"
            ].split(":")[-1]
        elif is_free and not is_bd_pro:
            # Get the coverage IDs
            coverage_result = get_id(
                email=email,
                password=password,
                query_class="allCoverage",
                query_parameters={
                    "$table_Id: ID": table_id,
                    "$isClosed: Boolean": False,
                },
                api_mode=api_mode,
                cloud_table=True,
            )
            if not coverage_result:
                raise ValueError("Coverage ID not found.")

            coverage_ids = coverage_result[0]
            log(coverage_ids)
            # Check if there are multiple coverage IDs
            if len(coverage_ids["data"]["allCoverage"]["edges"]) > 1:
                print(
                    "WARNING: Your table has more than one coverage. Only the first ID has been selected."
                )

            # Retrieve the first coverage ID
            coverage_id = coverage_ids["data"]["allCoverage"]["edges"][0]["node"][
                "id"
            ].split(":")[-1]
        # Return the 3 IDs in a dictionary
        if is_bd_pro and is_free:
            return {
                "table_id": table_id,
                "coverage_id": coverage_id,
                "coverage_id_pro": coverage_id_pro,
            }
        elif is_free and not is_bd_pro:
            return {
                "table_id": table_id,
                "coverage_id": coverage_id,
            }
        elif is_bd_pro and not is_free:
            return {
                "table_id": table_id,
                "coverage_id_pro": coverage_id_pro,
            }
    except Exception as e:
        print(f"Error occurred while retrieving IDs: {str(e)}")
        raise


def get_coverage_value(
    dataset_name: str,
    table_name: str,
    date_format: str,
    email: str,
    password: str,
    api_mode: str,
) -> dict:
    try:
        # get table ID in the PROD API
        table_result = get_id(
            email=email,
            password=password,
            query_class="allCloudtable",
            query_parameters={
                "$gcpDatasetId: String": dataset_name,
                "$gcpTableId: String": table_name,
            },
            cloud_table=True,
            api_mode=api_mode,
        )

        if not table_result:
            raise ValueError("Table ID not found.")

        table_id = table_result[0]["data"]["allCloudtable"]["edges"][0]["node"][
            "table"
        ].get("_id")

        # get coverage values in the PROD API for the table ID
        datetime_result = get_datetimerange(
            email=email,
            password=password,
            query_parameters={"$table_Id: ID": table_id},
            api_mode=api_mode,
        )

        if not datetime_result:
            raise ValueError("Datetime result not found.")

        date_objects = parse_datetime_ranges(datetime_result, date_format)
        return date_objects

    except Exception as e:
        log(
            f"Error occurred while retrieving Table IDs values or Coverage values from the PROD API: {str(e)}"
        )
        raise


def get_datetimerange(
    query_parameters: dict,
    email: str,
    password: str,
    api_mode: str = "prod",
) -> dict:
    """This function retrieves the datetimeRanges from the PROD API for the specified Table ID in the Query Parameters

    Args:
        query_parameters (dict): Used to filter the query results in GRAPHQL queries
        email (str): Email to access the API
        password (str): Password to access the API
        api_mode (str, optional): The API zone. Defaults to "prod".

    Returns:
        dict: A dictionary containing datetimeRanges from the PROD API
    """
    token = get_token(email, password, api_mode)
    header = {
        "Authorization": f"Bearer {token}",
    }

    # Extract table_id from query_parameters
    table_id = query_parameters.get("$table_Id: ID", None)

    query = """
    query($table_Id: ID) {
      allCoverage(table_Id: $table_Id) {
        edges {
          node {
            id
            datetimeRanges {
              edges {
                node {
                  id
                  startYear
                  startMonth
                  startDay
                  endYear
                  endMonth
                  endDay
                }
              }
            }
            table {
              _id
              name
              isClosed
              dataset {
                name
              }
            }
          }
        }
      }
    }
    """

    # Format the variables dictionary
    variables = {"table_Id": table_id}

    r = requests.post(
        url="https://api.basedosdados.org/api/v1/graphql",
        json={"query": query, "variables": variables},
        headers=header,
    ).json()

    return r


def parse_datetime_ranges(datetime_result: dict, date_format: str) -> dict:
    """This function parses datetime ranges from the PROD API and returns a dictionary with the coverage values. Used within get_datetimerange function.

    Args:
        datetime_result (dict): A dictionary with datetime ranges from the PROD API (given by the output of get_datetimerange function)
        date_format (str): A format string to correctly parse the datetime ranges

    Returns:
        dict: A dictionary with one or more datetime ranges for the given table
    """

    date_objects = {}

    log(f"the chosen format to parse the coverage was {date_format}")

    edges = datetime_result["data"]["allCoverage"]["edges"]

    # iterates over each edge
    for edge in edges:
        node = edge["node"]
        datetime_ranges = node["datetimeRanges"]["edges"]

        # iterates over each edge of datetime_ranges

        # ps: If the table has bd_pro coverage,
        # it will have more than one datetime_range
        for dt_range in datetime_ranges:
            dt_node = dt_range["node"]
            end_year = dt_node.get("endYear")
            end_month = dt_node.get("endMonth")
            end_day = dt_node.get("endDay")

            date_values = (end_year, end_month, end_day)

            date_string = format_check_date(date_values, date_format)
            log(f"The following coverage is being added {date_objects}")
            date_objects[dt_node["id"]] = date_string

    return date_objects


def format_check_date(date_values: tuple, date_format: str) -> str:
    """This function formats a date according to the given date_format and make 2 checks:
        1) If the date_format is valid for the given date_values; in other words, if the date_format is correct for the table.
        2) If the date_values are not NONE;

    Args:
        date_values (tuple): A tuple with date values
        date_format (str): The format string

    Raises:
        ValueError: Checks 1 and 2 for %Y-%m-%d date format
        ValueError: Checks 1 and 2 for %Y-%m date format
        ValueError: Checks 1 and 2 for %Y date format

    Returns:
        str: A string with a date in the format %Y-%m-%d, %Y-%m, %Y
    """

    end_year, end_month, end_day = date_values

    if date_format == "%Y-%m-%d":
        if end_year is not None and end_month is not None and end_day is not None:
            return f"{end_year:04d}-{end_month:02d}-{end_day:02d}"
        else:
            raise ValueError(
                f"Attention! The input date_format ->> {date_format} is wrong for the current Table. The input date_format was '%Y-%m-%d' but one of the elements | year ->> {end_year}, month ->> {end_month}, day ->> {end_day}  | have NONE values in the PROD API. If that's not the case, check the Coverage values in the Prod API, they may be not filled (NONE)"
            )

    elif date_format == "%Y-%m":
        if end_year is not None and end_month is not None:
            return f"{end_year:04d}-{end_month:02d}"
        else:
            raise ValueError(
                f"Attention! The input date_format ->> {date_format} is wrong for the current Table. The input date_format was '%Y-%m' but one of the elements | year ->> {end_year}, month ->> {end_month}| have NONE values in the PROD API. If that's not the case, check the Coverage values in the Prod API, they may be not filled (NONE)"
            )

    elif date_format == "Y%":
        if end_year is not None:
            return f"{end_year:04d}"
        else:
            raise ValueError(
                f"Attention! The input date_format ->> {date_format} is wrong for the current Table. The input date_format was 'Y%' but one of the elements | year ->> {end_year} | have NONE values in the PROD API. If that's not the case, check the Coverage values in the Prod API, they may be not filled (NONE)"
            )

def get_api_most_recent_date(
    dataset_id: str,
    table_id: str,
    date_format: str = "%Y-%m-%d",
    api_mode: str = "prod",
) -> datetime.date:
    """get the max table coverage for a given table id.

    This task will:

    1. Collect the credentials_utils;
    2. Access the PROD API and collect the DJANGO Table ID with the given GCS table ID;
    3. Collet all the end coverages for the given table_id;
    4. Parse the coverages;
    5. Compare the coverages and return the most recent date;

    Args:
        dataset_id (str): Dataset ID in GCS
        table_id (str): Table ID in GCS
        date_format (str, optional): Date format values %Y-%m-%d; %Y-%m; Y%
        api_mode (str, optional): Defaults to "prod".

    Raises:
        ValueError: if date_format is not valid

    Returns:
        str: a string representation of the date: %Y-%m-%d; %Y-%m; Y%
    """

    # check if date_format is valid
    accepted_date_format = ["%Y-%m-%d", "%Y-%m", "%Y"]
    # if not, raise ValueError
    if date_format not in accepted_date_format:
        raise ValueError(
            f"The date_format  ->>  ->> {date_format} is not supported. Please choose between {accepted_date_format}"
        )
    log(f"The chosen date_format is ->> {date_format}")

    # Collect credentials_utils
    (email, password) = get_credentials_utils(secret_path=f"api_user_{api_mode}")

    # collect all table coverages for the given table id
    coverages = get_coverage_value(
        dataset_id,
        table_id,
        date_format,
        email,
        password,
        api_mode,
    )

    # parse coverages
    log(f"For the table ->>{table_id} the parsed coverages were ->> {coverages}")

    # Convert the date strings to date objects
    date_objects = {}
    for key, date_string in coverages.items():
        date_objects[key] = datetime.strptime(date_string, date_format)

    # Compare the date objects, return the most recent date and format it
    log(
        f"For this table ->> {table_id} there are these datetime end values->> {date_objects}"
    )
    max_date_key = max(date_objects, key=date_objects.get)
    max_date_value = date_objects[max_date_key].strftime(date_format)

    log(
        f"The Most recent date for the ->> {table_id} in the prod API is ->> {max_date_value}"
    )

    return max_date_value