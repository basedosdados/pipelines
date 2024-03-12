# -*- coding: utf-8 -*-
import json
from typing import Dict

from basedosdados import backend as b
from link_directory_metadata import get_directory_column_id

from pipelines.utils.apply_architecture_to_dataframe.utils import (
    read_architecture_table,
)
from pipelines.utils.metadata.utils import get_headers


def create_column(
    backend: b.Backend,
    mutation_parameters: Dict[str, str] = None,
) -> bool:
    ## tinha que ser create or replace, por enquanto ele duplica
    ## os dados se rodar duas vezes por isso atenção na hora de rodar!

    # GraphQL mutation to create or update a column
    mutation = """
                    mutation($input: CreateUpdateColumnInput!) {
                        CreateUpdateColumn(input: $input) {
                            errors {
                                field,
                                messages
                            },
                            clientMutationId,
                            column {
                                id,
                            }
                        }
                    }
        """

    # Set headers for the GraphQL request, including the token for authentication
    headers = get_headers(backend)

    # Print the mutation parameters for debugging purposes
    pretty_json = json.dumps(mutation_parameters, indent=4)
    print(pretty_json)

    # Execute the GraphQL query with the provided mutation parameters and headers
    response = backend._execute_query(
        query=mutation, variables={"input": mutation_parameters}, headers=headers
    )

    # Print the response for debugging purposes
    if response["CreateUpdateColumn"]["errors"] != []:
        pretty_json = json.dumps(response, indent=4)
        print(pretty_json)
        return False

    return True


def get_column_id(table_id, column_name, url_api):
    backend = b.Backend(graphql_url=url_api)

    query = f"""{{
        allColumn(table_Id:"{table_id}", name:"{column_name}"){{
        edges{{
            node{{
            _id
            }}
        }}
        }}
    }}"""

    data = backend._execute_query(query=query)
    data = backend._simplify_graphql_response(response=data)["allColumn"]
    if data:
        return data[0]["_id"]
    else:
        print("column does not exists")


def get_n_columns(table_id, url_api):
    backend = b.Backend(graphql_url=url_api)

    query = f"""query get_n_columns{{
        allTable(id:"{table_id}"){{
            edges{{
            node{{
                columns{{
                edgeCount
                }}
            }}
            }}
        }}
        }}"""

    data = backend._execute_query(query=query)
    data = backend._simplify_graphql_response(response=data)["allTable"]

    return data[0]["columns"]["edgeCount"]


def get_bqtype_dict(url_api):
    # Initialize the backend object to interact with the GraphQL API
    backend = b.Backend(graphql_url=url_api)

    # GraphQL query to fetch all BigQuery types
    query = """{
    allBigquerytype{
      edges{
        node{
          name
          _id
        }
      }
    }
  }"""

    # Execute the GraphQL query to retrieve the data
    data = backend._execute_query(query=query)

    # Simplify the GraphQL response to extract the relevant data
    data = backend._simplify_graphql_response(response=data)["allBigquerytype"]

    # Create a dictionary where the 'name' part is the key and the '_id' is the value
    bqtype_dict = {item["name"]: item["_id"] for item in data}

    # Return the resulting dictionary
    return bqtype_dict


def check_metadata_columns(dataset_id, table_slug, url_api: str, url_architecture: str):
    # Create a backend object with the GraphQL URL
    # This will help us interact with the api
    backend = b.Backend(graphql_url=url_api)

    # Get the table ID using the dataset ID and table ID
    table_id = backend._get_table_id_from_name(gcp_dataset_id=dataset_id, gcp_table_id=table_slug)

    # Read the architecture table
    architecture = read_architecture_table(url_architecture=url_architecture)

    n_columns_metadata = get_n_columns(table_id=table_id, url_api=url_api)
    n_columns_architecture = architecture.shape[0]

    print(f"\nn_columns_metadata: {n_columns_metadata}")
    print(f"n_columns_architecture: {n_columns_architecture}")


def get_all_columns_id(table_id: str, backend: b.Backend):
    query = f"""{{
        allColumn(table_Id:"{table_id}"){{
        edges{{
            node{{
            _id
            }}
        }}
        }}
    }}"""

    data = backend._execute_query(query=query)
    columns_json = backend._simplify_graphql_response(response=data)["allColumn"]

    if data:
        columns_list = [col["_id"] for col in columns_json]
        return columns_list
    else:
        print("There is no column in this table to be deleted")


def delete_column_by_id(column_id: str, backend: b.Backend):
    mutation = """
                    mutation($input: UUID!) {
                        DeleteColumn(id: $input) {
                            errors,
                            ok
                        }
                    }
        """

    # Set headers for the GraphQL request, including the token for authentication
    headers = get_headers(backend)
    # Execute the GraphQL query with the provided mutation parameters and headers
    response = backend._execute_query(
        query=mutation, variables={"input": column_id}, headers=headers
    )

    # Print the response for debugging purposes
    if response["DeleteColumn"]["errors"] != []:
        pretty_json = json.dumps(response, indent=4)
        print(pretty_json)
        return False

    return True


def delete_all_columns(table_id: str, backend: b.Backend):
    columns = get_all_columns_id(table_id, backend)

    for col in columns:
        delete_column_by_id(col, backend)


def upload_columns_from_architecture(
    dataset_id: str,
    table_slug: str,
    url_architecture: str,
    if_column_exists: str = "pass",
    replace_all_schema: bool = True,
):
    """
    Uploads columns from an architecture table to the specified dataset and table in  platform.

    Notes:
    - This function assumes a specific structure/format for the architecture table.
    - It interacts with the Base dos Dados GraphQL API to create or update columns.
    - Columns from the architecture table are uploaded to the specified dataset and table.
    - It prints information about the existing columns
    and performs metadata checks after uploading columns.
    """
    accepted_if_exists_values = ["pass", "replace"]

    if if_column_exists not in accepted_if_exists_values:
        raise ValueError(f"`if_exists` only accepts {accepted_if_exists_values}")

    url_api = "https://api.basedosdados.org/api/v1/graphql"
    # Create a backend object with the GraphQL URL
    # This will help us interact with the api
    backend = b.Backend(graphql_url=url_api)

    # Get the table ID using the dataset ID and table ID
    table_id = backend._get_table_id_from_name(gcp_dataset_id=dataset_id, gcp_table_id=table_slug)
    print(f"table_id: {table_id}\n")

    # Read the architecture table
    architecture = read_architecture_table(url_architecture=url_architecture)

    # Get the id of BigQueryTypes in a dict
    bqtype_dict = get_bqtype_dict(url_api)

    if replace_all_schema:
        delete_all_columns(table_id, backend)

    # Iterate over each row in the 'architecture' DataFrame
    for _, row in architecture.iterrows():
        print(f"\nColumn: {row['name']}")

        column_id = get_column_id(table_id=table_id, column_name=row["name"], url_api=url_api)

        if column_id and if_column_exists == "pass":
            print("row already exists")
            continue

        # Define the mutation parameters for creating a new column
        directory_column_id = None
        if row["directory_column"]:
            directory_table_slug = row["directory_column"].split(":")[0].split(".")[1]
            directory_column_name = row["directory_column"].split(":")[1]
            directory_column_id = get_directory_column_id(
                directory_column_name, directory_table_slug, backend
            )

        mutation_parameters = {
            "table": table_id,
            "bigqueryType": bqtype_dict[row["bigquery_type"].upper()],
            "name": row["name"],
            "description": row["description"],
            "coveredByDictionary": row["covered_by_dictionary"] == "yes",
            "measurementUnit": row["measurement_unit"],
            "containsSensitiveData": row["has_sensitive_data"] == "yes",
            "observations": row["observations"],
            "directoryPrimaryKey": directory_column_id,
        }

        if column_id:
            mutation_parameters["id"] = column_id

        create_column(backend, mutation_parameters=mutation_parameters)

    check_metadata_columns(
        dataset_id=dataset_id,
        table_slug=table_slug,
        url_api=url_api,
        url_architecture=url_architecture,
    )

if __name__ == '__main__':

    upload_columns_from_architecture(
        dataset_id="<dataset_id>",
        table_slug="<table_slug>",
        url_architecture="<architecture_url>",
        replace_all_schema=True,
    )
