# -*- coding: utf-8 -*-
# import hvac
# from os import getenv

# def get_vault_client() -> hvac.Client:
#     """
#     Returns a Vault client.
#     """
#     client = hvac.Client(
#         url="https://vault.basedosdados.org/",
#         token="s.qf22k34trKKRfKNVfyDRCzD2",
#     )

#     return client


# def create_vault_client():
#     """
#     Creates a Vault client.
#     """
#     client = get_vault_client()

#     secret = {
#     "URL_DOWNLOAD_DATA_TEST": "gs://basedosdados-dev/datasets/teste/"
#     }
#     create_response = client.secrets.kv.v1.create_or_update_secret(path = "url_download_data_test", secret = secret)

#     return create_response

# create_vault_client()

import basedosdados as bd
from typing import Dict, List
from pipelines.utils.utils import log

def query_tables(days: int = 7, mode: str = "dev") -> List[Dict[str, str]]:
    """
    Queries BigQuery Tables metadata to find elegible tables to zip.
    """

    if mode == "dev":
        billing_project_id = "basedosdados-dev"
    elif mode == "prod":
        billing_project_id = "basedosdados"

    query = f"""
        SELECT
            dataset_id,
            table_id,
            row_count,
            size_bytes
        FROM `basedosdados.br_bd_metadados.bigquery_tables`
        WHERE
           dataset_id NOT IN ("analytics_295884852","logs") AND
           DATE_DIFF(CURRENT_DATE(),last_modified_date,DAY) <= {days}
    """

    tables = bd.read_sql(
        query=query,
        billing_project_id=billing_project_id,
        from_file=True,
    )

    log(f"Found {len(tables)} eligible tables to zip")

    to_zip = tables.to_dict("records")

    for key in range(len(to_zip) - 1):
        dataset_id = to_zip[key]["dataset_id"]
        table_id = to_zip[key]["table_id"]

        return dataset_id, table_id


my, mi = query_tables()
print(my)


from pipelines.utils.utils import run_local, run_cloud
from pipelines.utils.dump_to_gcs.flows import dump_to_gcs_flow
from pipelines.datasets.cross_update.tasks import get_all_tables_eligible_last_year
for tables, datasets in get_all_tables_eligible_last_year(days=1, mode="dev"):
        run_local(flow=dump_to_gcs_flow,
                parameters = {
                "dataset_id": datasets,
                "table_id": tables,
                "project_id": "basedosdados-dev",
                "billing_project_id":"basedosdados-dev"
                }
                )