# -*- coding: utf-8 -*-
"""
General purpose functions for the temporal_coverage_updater project
"""
import json
import requests
from datetime import datetime
import re
import basedosdados as bd
from pipelines.utils.temporal_coverage_updater.constants import (
    constants as temp_constants,
)
from typing import Tuple
from pipelines.utils.utils import log, get_credentials_from_secret


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
        `basedosdados.{dataset_id}.__TABLES__`
        WHERE
        table_id = '{table_id}'
        """
        # bd_base = Base()
        # billing_project_id = bd_base.config["gcloud-projects"]["prod"]["name"]
        t = bd.read_sql(
            query=query_bd,
            billing_project_id="basedosdados-dev",
            from_file=True,
        )
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


def get_credentials(secret_path: str) -> Tuple[str, str]:
    """
    Returns the user and password for the given secret path.
    """
    log(f"Getting user and password for secret path: {secret_path}")
    tokens_dict = get_credentials_from_secret(secret_path)
    email = tokens_dict.get("email")
    password = tokens_dict.get("password")
    return email, password


def get_token(email, password):
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
    return r.json()["data"]["tokenAuth"]["token"]


def get_id(
    query_class,
    query_parameters,
    email,
    password,
):  # sourcery skip: avoid-builtin-shadow
    # email = temp_constants.EMAIL.value
    # password = temp_constants.PASSWORD.value
    # backend = b.Backend(graphql_url="http://api.basedosdados.org/api/v1/graphql")
    token = get_token(email, password)
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

    query = f"""query({_filter}) {{
                        {query_class}({_input}){{
                        edges{{
                            node{{
                            id,
                            }}
                        }}
                        }}
                    }}"""
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
    query_class, query_parameters, email, password
):  # sourcery skip: avoid-builtin-shadow
    # email = temp_constants.EMAIL.value
    # password = temp_constants.PASSWORD.value
    # backend = b.Backend(graphql_url="http://api.basedosdados.org/api/v1/graphql")
    token = get_token(email=email, password=password)
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
):
    # email = temp_constants.EMAIL.value
    # password = temp_constants.PASSWORD.value
    # backend = b.Backend(graphql_url="http://api.basedosdados.org/api/v1/graphql")
    token = get_token(email=email, password=password)
    header = {
        "Authorization": f"Bearer {token}",
    }

    r, id = get_id(
        query_class=query_class,
        query_parameters=query_parameters,
        email=email,
        password=password,
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
    r = requests.post(
        "https://api.basedosdados.org/api/v1/graphql",
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
            # print(f"create: created {id}")
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
    elif len(temporal_coverage) == 4:
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

    return start_result


def get_ids(dataset_name: str, table_name: str, email: str, password: str) -> dict:
    """
    Obtains the IDs of the dataset, table, and coverage based on the provided names.

    Args:
        dataset_name (str): Name of the dataset.
        table_name (str): Name of the table.

    Returns:
        dict: Dictionary containing the 3 IDs.

    Raises:
        ValueError: If any of the IDs are not found.
        Exception: If an error occurs while retrieving the IDs.
    """
    try:
        # Get the dataset ID
        dataset_result = get_id(
            email=email,
            password=password,
            query_class="allDataset",
            query_parameters={"$slug: String": dataset_name},
        )
        if not dataset_result:
            raise ValueError("Dataset ID not found.")

        dataset_id = dataset_result[1]

        # Get the table ID
        table_result = get_id(
            email=email,
            password=password,
            query_class="allTable",
            query_parameters={
                "$slug: String": table_name,
                "$dataset_Id: ID": dataset_id,
            },
        )
        if not table_result:
            raise ValueError("Table ID not found.")

        table_id = table_result[1]

        # Get the coverage IDs
        coverage_result = get_id(
            email=email,
            password=password,
            query_class="allCoverage",
            query_parameters={"$table_Id: ID": table_id},
        )
        if not coverage_result:
            raise ValueError("Coverage ID not found.")

        coverage_ids = coverage_result[0]
        # print(coverage_ids)

        # Check if there are multiple coverage IDs
        if len(coverage_ids["data"]["allCoverage"]["edges"]) > 1:
            print(
                "WARNING: Your table has more than one coverage. Only the first ID has been selected."
            )

        # Retrieve the first coverage ID
        coverage_id = coverage_ids["data"]["allCoverage"]["edges"][0]["node"][
            "id"
        ].split(":")[-1]
        # print(coverage_id)
        # Return the 3 IDs in a dictionary
        return {
            "dataset_id": dataset_id,
            "table_id": table_id,
            "coverage_id": coverage_id,
        }
    except Exception as e:
        print(f"Error occurred while retrieving IDs: {str(e)}")
        raise
