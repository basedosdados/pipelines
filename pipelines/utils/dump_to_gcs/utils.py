# -*- coding: utf-8 -*-
from basedosdados import backend as bd
from basedosdados.download.base import google_client
from time import sleep
from pipelines.utils.utils import log
from pipelines.utils.dump_to_gcs.constants import constants as dump_to_gcs_constants
from google.cloud import bigquery
from google.cloud.bigquery import TableReference

def execute_query_in_bigquery(billing_project_id, query, path, location):
    client = google_client(billing_project_id, from_file=True, reauth=False)
    job = client["bigquery"].query(query)
    while not job.done():
        sleep(1)

    dest_table = job._properties["configuration"]["query"]["destinationTable"]
    dest_project_id = dest_table["projectId"]
    dest_dataset_id = dest_table["datasetId"]
    dest_table_id = dest_table["tableId"]
    log(f"Query results were stored in {dest_project_id}.{dest_dataset_id}.{dest_table_id}")
    blob_path = path
    log(f"Loading data to {blob_path}")
    dataset_ref = bigquery.DatasetReference(dest_project_id, dest_dataset_id)
    table_ref = dataset_ref.table(dest_table_id)
    job_config = bigquery.job.ExtractJobConfig(compression="GZIP")
    extract_job = client["bigquery"].extract_table(
        table_ref,
        blob_path,
        location=location,
        job_config=job_config,
    )
    return extract_job.result()