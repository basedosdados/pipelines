# -*- coding: utf-8 -*-
"""
Utils for cross_update pipeline
"""
import os

import pandas as pd
from pandas import json_normalize

from pipelines.utils.utils import get_credentials_from_secret, log
from pipelines.utils.metadata.utils import get_headers

def save_file(df: pd.DataFrame, table_id: str) -> str:
    """
    Saves a DataFrame as a CSV file.

    Args:
        df (pd.DataFrame): The DataFrame to be saved.
        table_id (str): The identifier for the table.

    Returns:
        str: The full file path of the saved CSV file.

    """

    # Define the folder path for storing the file
    folder = f"tmp/{table_id}"
    # Create the folder if it doesn't exist
    os.system(f"mkdir -p {folder}")
    # Define the full file path for the CSV file
    full_filepath = f"{folder}/{table_id}.csv"
    # Save the DataFrame as a CSV file
    df.to_csv(full_filepath, index=False)
    log("save_input")
    return full_filepath


def batch(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def find_closed_tables(backend):
    query = """
        query {
            allCoverage(isClosed: false, table_Id_Isnull:false, datetimeRanges_Id_Isnull:false) {
                edges {
                node {
                    table {
                        _id
                        name
                        isClosed
                        dataset {
                            name
                        }
                    }
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
                }
                }
            }
            }"""

    response = backend._execute_query(query=query)
    response = backend._simplify_graphql_response(response)["allCoverage"]
    data = json_normalize(response)
    open_tables = data["table._id"].tolist()

    query = """
        query {
            allCoverage(isClosed: true, table_Id_Isnull:false, datetimeRanges_Id_Isnull:false) {
                edges {
                node {
                    table {
                        _id
                        name
                        isClosed
                        dataset {
                            name
                        }
                    }
                    datetimeRanges {
                        edges {
                        node {
                            id
                        }
                        }
                        }
                }
                }
            }
            }"""

    response = backend._execute_query(query=query)
    response = backend._simplify_graphql_response(response)["allCoverage"]
    data = json_normalize(response)
    closed_tables = data["table._id"].tolist()

    all_closed_tables = [table for table in closed_tables if table not in open_tables]

    return all_closed_tables


def modify_table_metadata(table, backend):
    mutation = """
                    mutation($input: CreateUpdateTableInput!) {
                        CreateUpdateTable(input: $input) {
                            errors {
                                field,
                                messages
                            },
                            clientMutationId,
                            table {
                                id,
                            }
                        }
                    }
        """

    # colocando essa condição porque o graphql nao aceita valores maiores do que esse para os campos "uncompressedFileSize" e "numberRows"
    if table["size_bytes"] > 2147483647:
        table["size_bytes"] = None

    if table["row_count"] > 2147483647:
        table["row_count"] = None

    mutation_parameters = {
        "id": table["table_django_id"],
        "numberRows": table["row_count"],
        "uncompressedFileSize": table["size_bytes"],
    }

    response = backend._execute_query(
        query=mutation,
        variables={"input": mutation_parameters},
        headers=get_headers(backend),
    )

    if response is None:
        log(table)


