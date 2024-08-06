# -*- coding: utf-8 -*-
"""
Tasks for dumping data directly from BigQuery to GCS.
"""
from datetime import datetime
from time import sleep
from typing import Union
from basedosdados import backend as bd
from pipelines.utils.metadata.utils import get_api_most_recent_date, get_url
import jinja2
from basedosdados.download.base import google_client
from basedosdados.upload.base import Base
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from prefect import task
from google.cloud.bigquery import TableReference
from pipelines.utils.dump_to_gcs.utils import execute_query_in_bigquery
from pipelines.utils.dump_to_gcs.constants import constants as dump_to_gcs_constants
from pipelines.utils.utils import (
    determine_whether_to_execute_or_not,
    get_redis_client,
    log,
)


@task
def download_data_to_gcs(  # pylint: disable=R0912,R0913,R0914,R0915
    dataset_id: str,
    table_id: str,
    project_id: str = None,
    query: Union[str, jinja2.Template] = None,
    bd_project_mode: str = "prod",
    billing_project_id: str = None,
    location: str = "US",
):
    """
    Get data from BigQuery.

    As regras de negócio são:
        - Se a tabela for maior que 5GB: Não tem download disponível
        - Se a tabela for entre 100MB e 5GB: Tem downalod apenas para assinante BDPro
        - Se a tabela for menor que 100MB: Tem download para assinante BDPro e aberto
            - Se for parcialmente BDPro faz o download de arquivos diferentes para o público pagante e não pagante

    """
    # Try to get project_id from environment variable
    if not project_id:
        log("Project ID was not provided, trying to get it from environment variable")
        try:
            bd_base = Base()
            project_id = bd_base.config["gcloud-projects"][bd_project_mode]["name"]
        except KeyError:
            pass
        if not project_id:
            raise ValueError(
                "project_id must be either provided or inferred from environment variables"
            )
        log(f"Project ID was inferred from environment variables: {project_id}")

    # Asserts that dataset_id and table_id are provided
    if not dataset_id or not table_id:
        raise ValueError("dataset_id and table_id must be provided")

    # If query is not provided, build query from it
    if not query:
        query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"
        log(f"Query was inferred from dataset_id and table_id: {query}")

    # If query is not a string, raise an error
    if not isinstance(query, str):
        raise ValueError("query must be either a string or a Jinja2 template")
    log(f"Query was provided: {query}")

    # Get billing project ID
    if not billing_project_id:
        log(
            "Billing project ID was not provided, trying to get it from environment variable"
        )
        try:
            bd_base = Base()
            billing_project_id = bd_base.config["gcloud-projects"][bd_project_mode][
                "name"
            ]
        except KeyError:
            pass
        if not billing_project_id:
            raise ValueError(
                "billing_project_id must be either provided or inferred from environment variables"
            )
        log(
            f"Billing project ID was inferred from environment variables: {billing_project_id}"
        )

    # pylint: disable=E1124
    client = google_client(billing_project_id, from_file=True, reauth=False)

    bq_table_ref = TableReference.from_string(f'{project_id}.{dataset_id}.{table_id}')
    bq_table = client["bigquery"].get_table(table = bq_table_ref)
    num_bytes = bq_table.num_bytes
    log(f"Quantidade de linhas iniciais {bq_table.num_rows}")
    if num_bytes == 0:
        log("This table is views!")
        b = bd.Backend(graphql_url=get_url("prod"))
        django_table_id = b._get_table_id_from_name(gcp_dataset_id=dataset_id, gcp_table_id=table_id)
        query_graphql = f"""
        query get_bytes_table{{
        allTable(id: "{django_table_id}"){{
            edges{{
            node{{
                name
                uncompressedFileSize
                        }}}}
                    }}}}
            """
        data = b._execute_query(query_graphql, {'table_id' : table_id})
        nodes = data['allTable']['edges']
        if nodes == []:
            return None

        num_bytes = nodes[0]['node']['uncompressedFileSize']

    log(num_bytes)
    if num_bytes > 1_000_000_000:
        log("Table is bigger than 1GB it is not in the download criteria")
        return None

    if 100_000_000 <= num_bytes <= 1_000_000_000: # Entre 1 GB e 100 MB, apenas BD pro
        log("Querying data for BDpro user")

        blob_path = f"{dump_to_gcs_constants.secret_path_url_closed.value}{dataset_id}/{table_id}/{table_id}_bdpro.csv.gz"
        execute_query_in_bigquery(billing_project_id = billing_project_id,
                                query = query,
                                path = blob_path,
                                location = location)

        log("BDPro Data was loaded successfully")

    if num_bytes < 100_000_000: # valor menor que 100 MB

        # Try to remove bdpro access
        log("Trying to remove BDpro filter")
        try:
            query_remove_bdpro = f"DROP ROW ACCESS POLICY bdpro_filter ON `{project_id}.{dataset_id}.{table_id}`"
            log(query_remove_bdpro)
            job = client["bigquery"].query(query_remove_bdpro)
            while not job.done():
                sleep(1)

            log(job.result())

            log(
                "Table has BDpro filter and it was removed so direct download contains only open rows"
            )


            bdpro = True
        except NotFound as e:
            if "Not found: Row access policy bdpro_filter on table" in str(e):
                log("It was not possible to find BDpro filter, all rows will be downloaded")
                bdpro = False
            else:
                raise (e)
        except Exception as e:
            raise ValueError(e)

        log("this table is bdpro")

        log("Querying open data from BigQuery")

        blob_path = f"{dump_to_gcs_constants.secret_path_url_free.value}{dataset_id}/{table_id}/{table_id}.csv.gz"
        execute_query_in_bigquery(billing_project_id = billing_project_id,
                            query = query,
                            path = blob_path,
                            location = location)

        log("Open data was loaded successfully")

        if bdpro:
            query_restore_bdpro_access = f'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter  ON  `{project_id}.{dataset_id}.{table_id}` GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org") FILTER USING (TRUE)'

            log(query_restore_bdpro_access)
            job = client["bigquery"].query(query_restore_bdpro_access)

            while not job.done():
                sleep(1)
            log("BDpro filter was reestored")

            log("---------------")

            log("Querying Data closed from BigQuery")

            blob_path = f"{dump_to_gcs_constants.secret_path_url_closed.value}{dataset_id}/{table_id}/{table_id}_bdpro.csv.gz"
            execute_query_in_bigquery(billing_project_id = billing_project_id,
                                query = query,
                                path = blob_path,
                                location = location)
            log("Data closed was loaded successfully")

@task
def get_project_id(
    project_id: str = None,
    bd_project_mode: str = "prod",
):
    """
    Get the project ID.
    """
    if project_id:
        return project_id
    log("Project ID was not provided, trying to get it from environment variable")
    try:
        bd_base = Base()
        project_id = bd_base.config["gcloud-projects"][bd_project_mode]["name"]
    except KeyError:
        pass
    if not project_id:
        raise ValueError(
            "project_id must be either provided or inferred from environment variables"
        )
    log(f"Project ID was inferred from environment variables: {project_id}")
    return project_id


@task(nout=2)
def trigger_cron_job(
    project_id: str,
    dataset_id: str,
    table_id: str,
    cron_expression: str,
):
    """
    Tells whether to trigger a cron job.
    """
    redis_client = get_redis_client()
    key = f"{project_id}__{dataset_id}__{table_id}"
    log(f"Checking if cron job should be triggered for {key}")
    val = redis_client.get(key)
    current_datetime = datetime.now()
    if val and val is dict and "last_trigger" in val:
        last_trigger = val["last_trigger"]
        log(f"Last trigger: {last_trigger}")
        if last_trigger:
            return determine_whether_to_execute_or_not(
                cron_expression, current_datetime, last_trigger
            )
    log(f"No last trigger found for {key}")
    return True, current_datetime


@task
def update_last_trigger(
    project_id: str,
    dataset_id: str,
    table_id: str,
    execution_time: datetime,
):
    """
    Update the last trigger.
    """
    redis_client = get_redis_client()
    key = f"{project_id}__{dataset_id}__{table_id}"
    redis_client.set(key, {"last_trigger": execution_time})


