# -*- coding: utf-8 -*-
"""
Utils for cross_update pipeline
"""
import os
from typing import Tuple
import basedosdados as bd
from os import getenv
import hvac

import pandas as pd

from pipelines.utils.utils import log


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

    #colocando essa condição porque o graphql nao aceita valores maiores do que esse para os campos "uncompressedFileSize" e "numberRows" 
    if table['size'] > 2147483647:
        table['size'] = None

    if table['row_count'] > 2147483647:
        table['row_count'] = None


    mutation_parameters = {
            "id": table["table_django_id"],
            "numberRows": table["row_count"],
            "uncompressedFileSize": table['size']
        }

    response = backend._execute_query(query=mutation, variables={"input": mutation_parameters}, headers=get_headers(backend))

    if response is None:
        log(table)




def get_credentials_from_secret(
    secret_path: str,
    client: hvac.Client = None,
) -> dict:
    """
    Returns a username and password from a secret in Vault.
    """
    secret = get_vault_secret(secret_path, client)
    return secret["data"]

def get_vault_secret(secret_path: str, client: hvac.Client = None) -> dict:
    """
    Returns a secret from Vault.
    """
    vault_client = client if client else get_vault_client()
    return vault_client.secrets.kv.read_secret_version(secret_path)["data"]

def get_vault_client() -> hvac.Client:
    """
    Returns a Vault client.
    """
    return hvac.Client(
        url=getenv("VAULT_ADDRESS").strip(),
        token=getenv("VAULT_TOKEN").strip(),
    )


def get_headers(backend):
    credentials = get_credentials_from_secret(secret_path="api_user_prod")

    mutation = """
        mutation ($email: String!, $password: String!) {
            tokenAuth(email: $email, password: $password) {
                token
            }
        }
    """
    variables = {"email": credentials["email"], "password": credentials["password"]}

    response = backend._execute_query(query=mutation, variables=variables)
    token = response["tokenAuth"]["token"]
    
    header_for_mutation_query = {"Authorization": f"Bearer {token}"}

    return header_for_mutation_query
    
