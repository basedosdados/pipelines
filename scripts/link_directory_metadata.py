# -*- coding: utf-8 -*-
from typing import Optional

import basedosdados as bd
import pandas as pd

from pipelines.utils.metadata.utils import get_headers


def get_columns(column_name: str, backend: bd.Backend) -> pd.DataFrame:
    """
    Get the id from all columns that have a certain name
    """

    query = """ query($column_name: String)
        {allColumn(name_Icontains: $column_name) {
                edges {
                node {
                    name
                    _id
                    table {
                    dataset {
                        fullSlug
                    }
                    slug
                    }
                    directoryPrimaryKey {
                    table {
                        dataset {
                        fullSlug
                        }
                        slug
                    }
                    name
                    }
                }
                }
            }
            }
    """

    variables = {"column_name": column_name}

    response = backend._execute_query(query=query, variables=variables)
    response = backend._simplify_graphql_response(response)["allColumn"]

    df = pd.json_normalize(response)

    starts_with = df["name"].str.startswith(column_name + "_")
    ends_with = df["name"].str.endswith("_" + column_name)
    contains = df["name"].str.contains("_" + column_name + "_")
    is_exactly = df["name"].str == column_name

    selected = starts_with | ends_with | contains | is_exactly

    df = df[selected]

    return df


def get_directory_column_id(
    directory_column_name: str, directory_table_name: str, backend: bd.Backend
) -> str:
    """
    Get the directory id from that column
    """
    query = """ query($column_name: String)
        {allColumn(name: $column_name) {
                edges {
                node {
                    name
                    _id
                    table {
                    dataset {
                        fullSlug
                    }
                    slug
                    }
                }
                }
            }
            }
    """

    variables = {"column_name": directory_column_name}
    response = backend._execute_query(query=query, variables=variables)
    response = backend._simplify_graphql_response(response)["allColumn"]
    df = pd.json_normalize(response)

    colunas_de_diretorio = df["table.dataset.fullSlug"].str.contains("diretorios")
    for index, coluna in df[colunas_de_diretorio].iterrows():
        if coluna["table.slug"] == directory_table_name:
            print(
                f"\nConnecting to the directory column: \n\t{coluna['table.dataset.fullSlug']}.{coluna['table.slug']}:{coluna['name']} "
            )
            return coluna["_id"]

    raise (
        ValueError(
            f"\nWARNING - Unable to find the directory column with the following information: \n\tcolumn_name: {directory_column_name}  \n\ttable: {directory_table_name}"
        )
    )


def modify_directory_metadata(column_id: str, directory_id: str, backend) -> None:
    """
    modify directory metadata
    """
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
    variables = {"id": column_id, "directoryPrimaryKey": directory_id}

    headers = get_headers(backend)

    backend._execute_query(
        query=mutation, variables={"input": variables}, headers=headers
    )

    return None


def select_columns(
    df: pd.DataFrame, not_column_name: list, remove_already_done: bool = False
) -> list:
    descricao = df["name"].str.contains("descricao")
    diretorio = df["table.dataset.fullSlug"].str.contains("diretorios")

    print(f"\n Removed {descricao.sum()} description columns ")
    print(f" Removed {diretorio.sum()} directory columns ")

    df = df[~(descricao | diretorio)]

    for wrong_column_name in not_column_name:
        contains_wrong_column_name = df["name"].str.contains(wrong_column_name)
        print(
            f" Removed {contains_wrong_column_name.sum()} columns with {wrong_column_name}"
        )
        df = df[~contains_wrong_column_name]

    if remove_already_done and ("directoryPrimaryKey.name" in df.columns):
        conected = df["directoryPrimaryKey.name"] != None
        print(
            f" Removed {conected.sum()} columns that are already conected to a directory"
        )
        df = df[~conected]

    print("-------------------")
    print(f" Remains {len(df)} columns to conect to the directory:")

    df["column_identifier"] = (
        df["table.dataset.fullSlug"].astype(str)
        + "-"
        + df["table.slug"].astype(str)
        + "."
        + df["name"].astype(str)
    )

    valores_ordenados = df["column_identifier"].sort_values()

    for coluna in valores_ordenados:
        print(f"\t- {coluna}")

    return df["_id"].values.tolist()


def link_directory_metadata(
    matching_column_pattern: str,
    directory_column_name: Optional[str],
    directory_table_name: Optional[str],
    not_matching_pattern: list,
    ignore_previously_linked_columns: bool,
    ready_to_change_metadata: bool,
) -> None:
    """
    This function searches for columns within the metadata that match the specified `matching_column_pattern`.
    It performs filtering based on defined criteria and presents a list of selected columns.
    If `ready_to_change_metadata` is True, modifies the metadata

    Filters out columns based on the following criteria:
        - Represents directories
        - Serves as description columns
        - Contains strings listed in `not_matching_pattern`

    Usage Recommendations:
    - Run the function with `ready_to_fix_metadata` set to False to:
        - Review columns for potential connection to the directory.
        - Validate the correctness of the directory column.

    Note:
    - The `directory_column_name` and `directory_table_name` default to `matching_column_pattern` if not provided.

    """
    backend = bd.Backend(graphql_url="https://api.basedosdados.org/api/v1/graphql")

    if directory_column_name is None:
        directory_column_name = matching_column_pattern

    if directory_table_name is None:
        directory_table_name = matching_column_pattern

    directory_column_id = get_directory_column_id(
        directory_column_name, directory_table_name, backend
    )

    df = get_columns(matching_column_pattern, backend)

    print(f"\nFound {len(df)} columns that include '{matching_column_pattern}'")

    columns_list = select_columns(
        df, not_matching_pattern, ignore_previously_linked_columns
    )

    if ready_to_change_metadata:
        for column in columns_list:
            modify_directory_metadata(column, directory_column_id, backend)
        print(f"\n{len(columns_list)} columns were connected to directory")
