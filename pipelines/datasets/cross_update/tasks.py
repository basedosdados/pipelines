# -*- coding: utf-8 -*-
"""
Tasks for cross update of metadata.
"""
import re

# pylint: disable=invalid-name, too-many-locals
from typing import Dict, List

import basedosdados as bd
import pandas as pd
from google.cloud import storage
from prefect import task
from tqdm import tqdm

from pipelines.datasets.cross_update.utils import (
    find_closed_tables,
    modify_table_metadata,
    save_file,
)
from pipelines.utils.utils import log


@task
def query_tables(month: int = 7, mode: str = "dev") -> List[Dict[str, str]]:
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
           last_modified_date >= '2023-{month}-01' AND
           last_modified_date < '2023-{month+1}-01'
    """

    tables = bd.read_sql(
        query=query,
        billing_project_id=billing_project_id,
        from_file=True,
    )

    log(f"Found {len(tables)} tables to update_metadata")

    to_zip = tables.to_dict("records")

    log(to_zip)

    return to_zip


@task
def rename_blobs() -> None:
    """Rename blobs in basedosdados-dev bucket"""
    storage_client = storage.Client()

    bucket = storage_client.get_bucket("basedosdados-public")
    blobs = bucket.list_blobs(prefix="one-click-download/")

    for blob in tqdm(list(blobs)):
        table_id = blob.name.split("/")[-2]
        new_name = re.sub(r"data0*\.csv\.gz", table_id + ".csv.gz", blob.name)
        bucket.rename_blob(blob, new_name)


@task
def get_metadata_data(mode: str = "dev"):
    if mode == "dev":
        billing_project_id = "basedosdados-dev"
    elif mode == "prod":
        billing_project_id = "basedosdados"

    schema_names_query = """
            SELECT  schema_name FROM  `basedosdados`.INFORMATION_SCHEMA.SCHEMATA
    """

    log(schema_names_query)
    schema_names_list = bd.read_sql(
        query=schema_names_query,
        billing_project_id=billing_project_id,
        from_file=True,
    )["schema_name"]

    def batch(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i : i + n]

    df_list = []
    for schema_batch in batch(schema_names_list, 50):
        query = ""
        for schema_name in schema_batch:
            query += f"SELECT * FROM `basedosdados.{schema_name}.__TABLES__` UNION ALL "
        query = query[:-11]
        batch_df = bd.read_sql(
            query=query,
            billing_project_id=billing_project_id,
            from_file=True,
        )
        df_list.append(batch_df)

    df = pd.concat(df_list, ignore_index=True)

    full_filepath = save_file(df=df, table_id="bigquery_tables")

    return full_filepath


@task
def update_metadata_and_filter(eligible_download_tables):
    """Get only tables"""

    backend = bd.Backend(graphql_url="https://api.basedosdados.org/api/v1/graphql")
    all_closed_tables = find_closed_tables(backend)
    remove_from_eligible_download_table = []

    for table in eligible_download_tables:
        table["table_django_id"] = backend._get_table_id_from_name(
            gcp_dataset_id=table["dataset_id"], gcp_table_id=table["table_id"]
        )

        if table["table_django_id"] is None:
            remove_from_eligible_download_table.append(table)

        elif table["row_count"] > 200000 or table["size_bytes"] > 5000000000 :
            remove_from_eligible_download_table.append(table)
            log(f"{table['dataset_id']}.{table['table_id']} is too big to zip")

        elif table["table_django_id"] in all_closed_tables:
            remove_from_eligible_download_table.append(table)
            log(f"{table['dataset_id']}.{table['table_id']} is not in open_tables")

        # if table["table_django_id"] is not None:
        #     modify_table_metadata(table, backend)

    log(f"Removed {len(remove_from_eligible_download_table)} tables")
    eligible_download_tables = [
        table
        for table in eligible_download_tables
        if table not in remove_from_eligible_download_table
    ]

    log(f"Found {len(eligible_download_tables)} tables to zip")
    for table in eligible_download_tables:
        log(table)

    return eligible_download_tables
