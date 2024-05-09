# -*- coding: utf-8 -*-
import basedosdados as bd
import pandas as pd
from upload_columns import upload_columns_from_architecture

from pipelines.utils.metadata.utils import create_update


def get_publisher_id(dataset_django_id: str, backend: bd.Backend):
    query = """query get_publisher_id($dataset_id: ID){
                allTable(dataset_Id: $dataset_id){
                    edges{
                    node{
                        publishedBy{
                        id
                        }
                    }
                    }
                }
                }"""
    query_data = backend._execute_query(query=query, variables={"dataset_id": dataset_django_id})
    clean_data = query_data["allTable"]["edges"]
    df = pd.json_normalize(clean_data)
    publisher_id = df["node.publishedBy.id"].mode().values[0].split(":")[1]

    return publisher_id


def create_dictionary_table_metadata(dataset_id: str, backend: bd.Backend) -> str:
    dataset_django_info = backend.get_dataset_config(dataset_id=dataset_id)
    dataset_django_id = backend._get_dataset_id_from_name(gcp_dataset_id=dataset_id)

    # publisher_id = get_publisher_id(dataset_django_id, backend)
    new_dict_parameters = {
        "slug": "dicionario",
        "namePt": "Dicionário",
        "descriptionPt": f"Dicionário para tradução dos códigos das tabelas do do conjunto {dataset_django_info['name']}. Para códigos definidos por outras instituições, como id_municipio ou cnaes, buscar por diretórios",
        "dataset": dataset_django_id,
        # "publishedBy": publisher_id,
        # "dataCleanedBy": publisher_id,
    }

    query_parameters = {"$dataset_Id: ID": dataset_django_id, "$slug: String": "dicionario"}

    _, id = create_update(
        mutation_class="CreateUpdateTable",
        mutation_parameters=new_dict_parameters,
        query_class="allTable",
        query_parameters=query_parameters,
        backend=backend,
    )
    return id


def create_dictionary_cloud_table_metadata(
    table_django_id: str, dataset_id: str, backend: bd.Backend
) -> str:
    query_class = "allCloudtable"
    query_parameters = {"$gcpDatasetId: String": dataset_id, "$gcpTableId: String": "dicionario"}

    mutation_class = "CreateUpdateCloudTable"
    mutation_parameters = {
        "gcpProjectId": "basedosdados",
        "gcpDatasetId": dataset_id,
        "gcpTableId": "dicionario",
        "table": table_django_id,
    }

    _, id = create_update(
        mutation_class, mutation_parameters, query_class, query_parameters, backend
    )

    return id


def create_dictionary_columns_metadata(dataset_id: str, backend: bd.Backend):
    upload_columns_from_architecture(
        dataset_id=dataset_id,
        table_slug="dicionario",
        url_architecture="https://docs.google.com/spreadsheets/d/1Xjo5Euj8sFZ_EpFfWYge9kjqEHEyOyyl0DUJFyQTfyM/edit#gid=0",
        backend=backend,
    )


def create_dict_metadata(dataset_id: str, backend: bd.Backend):
    table_django_id = create_dictionary_table_metadata(dataset_id, backend)
    create_dictionary_cloud_table_metadata(table_django_id, dataset_id, backend)
    create_dictionary_columns_metadata(dataset_id, backend)


if __name__ == "__main__":
    dataset = "<dataset>"
    backend = bd.Backend("https://api.basedosdados.org/api/v1/graphql")
    create_dict_metadata(dataset_id=dataset, backend=backend)
