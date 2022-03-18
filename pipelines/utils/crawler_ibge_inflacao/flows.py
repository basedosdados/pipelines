"""
Flows for br_ibge_inpc
"""
# pylint: disable=C0103, E1123, invalid-name, R0801

from prefect import Flow, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.utils.tasks import upload_to_gcs, create_bd_table, dump_header_to_csv
from pipelines.utils.crawler_ibge_inflacao.tasks import (
    crawler,
    clean_mes_brasil,
    clean_mes_rm,
    clean_mes_municipio,
    clean_mes_geral,
)


with Flow("Template: Ingerir tabela Inflacao") as flow_ibge_inflacao_mes_brasil:

    #####################################
    #
    # Parameters
    #
    #####################################

    INDICE = Parameter("indice")
    FOLDER = Parameter("indice")
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")

    wait_crawler = crawler(indice=INDICE, folder=FOLDER)
    # pylint: disable=E1123
    filepath = clean_mes_brasil(indice=INDICE, upstream_tasks=[wait_crawler])

    wait_header_path = dump_header_to_csv(data_path=filepath, wait=filepath)

    # Create table in BigQuery
    wait_create_bd_table = create_bd_table(  # pylint: disable=invalid-name
        path=wait_header_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_type="overwrite",
        wait=wait_header_path,
    )

    # Upload to GCS
    upload_to_gcs(
        path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        wait=wait_create_bd_table,
    )

flow_ibge_inflacao_mes_brasil.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_ibge_inflacao_mes_brasil.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)


with Flow("Template: Ingerir tabela Inflacao") as flow_ibge_inflacao_mes_rm:

    #####################################
    #
    # Parameters
    #
    #####################################

    INDICE = Parameter("indice")
    FOLDER = Parameter("indice")
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")

    wait_crawler = crawler(indice=INDICE, folder=FOLDER)
    # pylint: disable=E1123
    filepath = clean_mes_rm(indice=INDICE, upstream_tasks=[wait_crawler])

    wait_header_path = dump_header_to_csv(data_path=filepath, wait=filepath)

    # Create table in BigQuery
    wait_create_bd_table = create_bd_table(  # pylint: disable=invalid-name
        path=wait_header_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_type="overwrite",
        wait=wait_header_path,
    )

    # Upload to GCS
    upload_to_gcs(
        path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        wait=wait_create_bd_table,
    )

flow_ibge_inflacao_mes_rm.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_ibge_inflacao_mes_rm.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)


with Flow("Template: Ingerir tabela Inflacao") as flow_ibge_inflacao_mes_municipio:

    #####################################
    #
    # Parameters
    #
    #####################################

    INDICE = Parameter("indice")
    FOLDER = Parameter("indice")
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")

    wait_crawler = crawler(indice=INDICE, folder=FOLDER)
    # pylint: disable=E1123
    filepath = clean_mes_municipio(indice=INDICE, upstream_tasks=[wait_crawler])

    wait_header_path = dump_header_to_csv(data_path=filepath, wait=filepath)

    # Create table in BigQuery
    wait_create_bd_table = create_bd_table(  # pylint: disable=invalid-name
        path=wait_header_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_type="overwrite",
        wait=wait_header_path,
    )

    # Upload to GCS
    upload_to_gcs(
        path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        wait=wait_create_bd_table,
    )

flow_ibge_inflacao_mes_municipio.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_ibge_inflacao_mes_municipio.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)


with Flow("Template: Ingerir tabela Inflacao") as flow_ibge_inflacao_mes_geral:

    #####################################
    #
    # Parameters
    #
    #####################################

    INDICE = Parameter("indice")
    FOLDER = Parameter("indice")
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")

    wait_crawler = crawler(indice=INDICE, folder=FOLDER)
    # pylint: disable=E1123
    filepath = clean_mes_geral(indice=INDICE, upstream_tasks=[wait_crawler])

    wait_header_path = dump_header_to_csv(data_path=filepath, wait=filepath)

    # Create table in BigQuery
    wait_create_bd_table = create_bd_table(  # pylint: disable=invalid-name
        path=wait_header_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_type="overwrite",
        wait=wait_header_path,
    )

    # Upload to GCS
    upload_to_gcs(
        path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        wait=wait_create_bd_table,
    )

flow_ibge_inflacao_mes_geral.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_ibge_inflacao_mes_geral.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
