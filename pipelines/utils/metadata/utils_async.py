# -*- coding: utf-8 -*-

###
## Módulo de funções para fazer interagir de forma assíncrona com a api.
###

import asyncio
import aiohttp
import json
import re
from pipelines.utils.metadata.utils import get_id, get_token
from pipelines.utils.utils import get_credentials_from_secret, log, get_credentials_utils
import pandas as pd


async def create_update_async(
    email: str,
    password: str,
    mutation_class: str,
    mutation_parameters: dict,
    query_class: str,
    query_parameters: dict,
    update: bool = True,
    api_mode: str = "prod",
) -> tuple:
    """
    Function to create or update data using GraphQL mutations asynchronously.

    Parameters:
    - email (str): The email address used for authentication.
    - password (str): The password used for authentication.
    - mutation_class (str): The name of the GraphQL mutation class.
    - mutation_parameters (dict): The parameters for the GraphQL mutation.
    - query_class (str): The name of the GraphQL query class.
    - query_parameters (dict): The parameters for the GraphQL query.
    - update (bool): Indicates whether to update existing data (default is True).
    - api_mode (str): Specifies the API mode, either 'prod' (default) or 'staging'.

    Returns:
    - tuple: A tuple containing the response from the API and the ID of the created or updated data.

    Raises:
    - Exception: If there is an error during the GraphQL operation.

    Note:
    - This function performs asynchronous operations using aiohttp.ClientSession.
    - It authenticates using the provided email and password to obtain a token.
    - It constructs a GraphQL mutation query based on the provided mutation class and parameters.
    - If updating data and an ID is available, it includes the ID in the mutation parameters.
    - It sends the mutation request to the specified API endpoint.
    - It handles the response from the API, checking for errors and extracting the ID if successful.
    - If there is an error in the response, it raises an Exception with details.
    """
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
            print(r, id)

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
        url = "https://api.basedosdados.org/api/v1/graphql"
    elif api_mode == "staging":
        url = "https://staging.api.basedosdados.org/api/v1/graphql"

    async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                json={"query": query, "variables": {"input": mutation_parameters}},
                headers=header,
            ) as response:
                r = await response.json()
                log(f"Response: {r}")

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



async def create_quality_check_async(
    email: str,
    password: str,
    name: str,
    description: str,
    passed: str,
    dataset_id: str,
    table_id: str,
    api_mode: str = "prod",
) -> None:
    """
    Function to create or update a quality check for a dataset table asynchronously.

    Parameters:
    - email (str): The email address used for authentication.
    - password (str): The password used for authentication.
    - name (str): The name of the quality check.
    - description (str): The description of the quality check.
    - passed (str): Indicates whether the quality check passed ('pass') or failed ('fail').
    - dataset_id (str): The ID of the dataset containing the table.
    - table_id (str): The ID of the table for which the quality check is created or updated.
    - api_mode (str): Specifies the API mode, either 'prod' (default) or 'staging'.

    Returns:
    - None

    Raises:
    - ValueError: If the table ID is not found.
    """
    table_result, id = get_id(
        email=email,
        password=password,
        query_class="allCloudtable",
        query_parameters={
            "$gcpDatasetId: String": dataset_id,
            "$gcpTableId: String": table_id,
        },
        cloud_table=True,
        api_mode=api_mode,
    )

    result_id = table_result["data"]["allCloudtable"]["edges"][0]["node"][
        "table"
    ].get("_id")

    quality_check, quality_check_id = get_id(
        email=email,
        password=password,
        query_class="allQualitycheck",
        query_parameters={
            "$name: String": name,
            "$table_Id: ID": result_id,

        },
        cloud_table=False,
        api_mode=api_mode,
    )

    if not id:
        raise ValueError("Table ID not found.")

    parameters = {
                            "name": name,
                            "description": description,
                            "passed": True if passed == 'pass' else False,
                            "table": result_id
        }

    await create_update_async(
        query_class="allQualitycheck",
        query_parameters={"$id: ID": quality_check_id},
        mutation_class="CreateUpdateQualityCheck",
        mutation_parameters=parameters,
        update=True,
        email=email,
        password=password,
        api_mode=api_mode,
)



async def create_update_quality_checks_async(
    tests_results: pd.DataFrame,
    api_mode: str = "prod"
) -> None:
    """
    Function to create or update multiple quality checks asynchronously based on test results.

    Parameters:
    - tests_results (pd.DataFrame): A pandas DataFrame containing test results.
    - api_mode (str): Specifies the API mode, either 'prod' (default) or 'staging'.

    Returns:
    - None
    """
    (email, password) = get_credentials_utils(secret_path=f"api_user_{api_mode}")
    semaphore = asyncio.Semaphore(16)
    async with semaphore:
        tasks = [create_quality_check_async(email = email, password = password, name =row['name'],description= row['description'], passed =row['status'], dataset_id = row['dataset_id'], table_id= row['table_id']) for index, row in tests_results.iterrows() ]
        await asyncio.gather(*tasks)