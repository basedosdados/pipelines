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
    token,
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
    try:
        table_result, id = await get_id_async(
            email=email,
            password=password,
            query_class="allCloudtable",
            query_parameters={
                "$gcpDatasetId: String": dataset_id,
                "$gcpTableId: String": table_id,
            },
            cloud_table=True,
            api_mode=api_mode,
            token = token
        )

        if not id:
            log(f"{table_id} ID not found.")
            slug = dataset_id.split("_")[-1]
            log(f"Usando esse slug {slug}")
            table_result, id = await get_id_async(
                email=email,
                password=password,
                query_class="allTable",
                query_parameters={
                    "$dataset_Slug_Iendswith: String": slug,
                    "$name: String": table_id,
                },
                cloud_table=False,
                api_mode=api_mode,
                token = token
                )
            log(id)

    except ValueError as e:
        log(str(e))
        return


    quality_check, quality_check_id = await get_id_async(
        email=email,
        password=password,
        query_class="allQualitycheck",
        query_parameters={
            "$name: String": name,
            "$table_Id: ID": id,

        },
        cloud_table=False,
        api_mode=api_mode,
        token = token
    )

    parameters = {
                            "name": name,
                            "description": description,
                            "passed": True if passed == 'pass' else False,
                            "table": id
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
    token = get_token(email, password, api_mode)
    async with semaphore:
        tasks = [create_quality_check_async(email = email, password = password, name =row['name'],description= row['description'], passed =row['status'], dataset_id = row['dataset_id'], table_id= row['table_id'], token = token) for index, row in tests_results.iterrows() ]
        await asyncio.gather(*tasks)


async def get_id_async(
    query_class,
    query_parameters,
    email,
    password,
    token,
    api_mode: str = "prod",
    cloud_table: bool = True,
):
    async with aiohttp.ClientSession() as session:

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
            url = "https://staging.api.basedosdados.org/api/v1/graphql"
        elif api_mode == "prod":
            url = "https://api.basedosdados.org/api/v1/graphql"

        async with session.post(
            url=url,
            json={"query": query, "variables": dict(zip(keys, values))},
            headers=header,
        ) as response:
            r = await response.json()

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
                log(r)
                raise Exception(
                    f"Error: the executed query did not return a data json.\nExecuted query:\n{query} \nVariables: {dict(zip(keys, values))}"
                )