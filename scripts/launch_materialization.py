# -*- coding: utf-8 -*-
import basedosdados as bd
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
    return query_data


def launch_materialization(dataset_id: str, table_id: str, alias: bool = True):

    alias = True

    print(
        f"Launching materialization flow for {dataset_id}.{table_id} (alias={alias})..."
    )
    parameters = {
        "dataset_id": dataset_id,
        "dbt_alias": alias,
        "mode": "dev",
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
        "label": "basedosdados-dev",
    }

    print(variables)

    response = client.graphql(
        query = mutation,
        variables = variables
    )

    print(response)



if __name__ == '__main__':
    client = Client()

    # backend = bd.Backend(graphql_url='https://api.basedosdados.org/api/v1/graphql')

    # datasets_with_dictionary = get_dataset_with_dictionary(backend)
    table_id = 'dicionario'

    for dataset_id in ['br_ibge_pof']:
        launch_materialization(dataset_id, table_id)