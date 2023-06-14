# -*- coding: utf-8 -*-
"""
General purpose functions for the temporal_coverage_updater project
"""
import os
import json
import requests
from datetime import datetime
import re
from basedosdados import backend as b


def get_token(backend, email: str, password: str) -> str:
    """
    Get JWT token for authentication to be able to edit metadata directly from api
    """
    mutation = """
        mutation ($email: String!, $password: String!) {
            tokenAuth(email: $email, password: $password) {
                token
            }
        }
    """
    variables = {"email": email, "password": password}
    response = backend._execute_query(mutation, variables)
    return response["tokenAuth"]["token"]


def get_id(
    query_class,
    query_parameters,
):  # sourcery skip: avoid-builtin-shadow
    EMAIL = os.environ["API_EMAIL"]
    PASSWORD = os.environ["API_PASSWORD"]
    backend = b.Backend(graphql_url="http://api.basedosdados.org/api/v1/graphql")
    token = get_token(backend, email=EMAIL, password=PASSWORD)
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


def get_date(query_class, query_parameters):  # sourcery skip: avoid-builtin-shadow
    EMAIL = os.environ["API_EMAIL"]
    PASSWORD = os.environ["API_PASSWORD"]
    backend = b.Backend(graphql_url="http://api.basedosdados.org/api/v1/graphql")
    token = get_token(backend, email=EMAIL, password=PASSWORD)
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
    mutation_class,
    mutation_parameters,
    query_class,
    query_parameters,
    update=False,
):
    EMAIL = os.environ["API_EMAIL"]
    PASSWORD = os.environ["API_PASSWORD"]
    backend = b.Backend(graphql_url="http://api.basedosdados.org/api/v1/graphql")
    token = get_token(backend, email=EMAIL, password=PASSWORD)
    header = {
        "Authorization": f"Bearer {token}",
    }

    r, id = get_id(
        query_class=query_class, query_parameters=query_parameters, header=header
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
            result[f"{position}Day"] = date.month
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


def get_ids(dataset_name: str, table_name: str) -> dict:
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
            query_class="allDataset", query_parameters={"$slug: String": dataset_name}
        )
        if not dataset_result:
            raise ValueError("Dataset ID not found.")

        dataset_id = dataset_result[1]

        # Get the table ID
        table_result = get_id(
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
            query_class="allCoverage", query_parameters={"$table_Id: ID": table_id}
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
        raise Exception(f"Error occurred while retrieving IDs: {str(e)}")
