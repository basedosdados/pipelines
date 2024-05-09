# -*- coding: utf-8 -*-
import basedosdados as bd
import pandas as pd
from prefect import Client


### pra esse código funcionar precisa:
# ativar a .venv
#  source /home/laura/Documents/pipelines/.venv/bin/activate
#  source /home/laura/Documents/pipelines/.env
def get_dataset_with_dictionary(
    backend,
):
    query = """ {
            allTable(slug:"dicionario"){
                edges{
                node{
                    cloudTables{
                    edges{
                    node{
                        gcpDatasetId
                    }
                    }
                    }
                }
                }
            }
            }"""

    query_data = backend._execute_query(
        query=query,
    )

    ugly_list = pd.json_normalize(query_data["allTable"]["edges"])["node.cloudTables.edges"]
    pretty_list = [x[0]["node"]["gcpDatasetId"] for x in ugly_list if x]
    return pretty_list


def launch_materialization(dataset_id: str, table_id: str, alias: bool = True):
    alias = True

    print(f"Launching materialization flow for {dataset_id}.{table_id} (alias={alias})...")
    parameters = {
        "dataset_id": dataset_id,
        "dbt_alias": alias,
        "mode": "prod",
        "table_id": table_id,
    }

    mutation = """
    mutation ($flow_id: UUID, $parameters: JSON, $label: String!) {
        create_flow_run (input: {
            flow_id: $flow_id,
            parameters: $parameters,
            labels: [$label],
        }) {
            id
        }
    }
    """
    variables = {
        "flow_id": "bd6e69be-f5fc-4933-bae2-4e426c5bd5fc",
        "parameters": parameters,
        "label": "basedosdados",
    }

    print(variables)

    response = client.graphql(query=mutation, variables=variables)

    print(response)


if __name__ == "__main__":
    client = Client()

    backend = bd.Backend(graphql_url="https://api.basedosdados.org/api/v1/graphql")

    datasets_with_dictionary = get_dataset_with_dictionary(backend)
    table_id = "dicionario"

    for dataset_id in datasets_with_dictionary:
        launch_materialization(dataset_id, table_id)
