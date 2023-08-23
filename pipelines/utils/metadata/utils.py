# -*- coding: utf-8 -*-
"""
General purpose functions for the metadata project
"""

import json

# pylint: disable=too-many-arguments
from datetime import datetime
from os import getenv, walk
from typing import Any, Dict, List, Tuple

import basedosdados as bd
import numpy as np
import pandas as pd
import requests
from pipelines.utils.utils import log, get_credentials_from_secret


import re


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
        r = requests.post(
            "http://api.basedosdados.org/api/v1/graphql",
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
    elif api_mode == "staging":
        r = requests.post(
            "http://staging.api.basedosdados.org/api/v1/graphql",
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
    padrao_ano = r"\d{4}\(\d{1,2}\)\d{4}"
    padrao_mes = r"\d{4}-\d{2}\(\d{1,2}\)\d{4}-\d{2}"
    padrao_semana = r"\d{4}-\d{2}-\d{2}\(\d{1,2}\)\d{4}-\d{2}-\d{2}"
    padrao_dia = r"\d{4}-\d{2}-\d{2}\(\d{1,2}\)\d{4}-\d{2}-\d{2}"

    if (
        re.match(padrao_ano, temporal_coverage)
        or re.match(padrao_mes, temporal_coverage)
        or re.match(padrao_semana, temporal_coverage)
        or re.match(padrao_dia, temporal_coverage)
    ):
        print("A data está no formato correto.")
    else:
        print("Aviso: A data não está no formato correto.")

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
