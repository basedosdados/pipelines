"""
Flows for ibge inflacao
"""
# pylint: disable=C0103, E1123, invalid-name, duplicate-code, R0801

from prefect import Flow, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.utils.tasks import create_table_and_upload_to_gcs
from pipelines.utils.crawler_ibge_inflacao.tasks import (
    crawler,
    clean_mes_brasil,
    clean_mes_rm,
    clean_mes_municipio,
    clean_mes_geral,
)


with Flow("Template: IBGE Inflação: mes_brasil") as flow_ibge_inflacao_mes_brasil:

    #####################################
    #
    # Parameters
    #
    #####################################

    INDICE = Parameter("indice")
    FOLDER = Parameter("folder")
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")

    wait_crawler = crawler(indice=INDICE, folder=FOLDER)
    # pylint: disable=E1123
    filepath = clean_mes_brasil(indice=INDICE, upstream_tasks=[wait_crawler])

    create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_type="overwrite",
        wait=filepath,
    )

flow_ibge_inflacao_mes_brasil.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_ibge_inflacao_mes_brasil.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)


with Flow("Template: IBGE Inflação: mes_rm") as flow_ibge_inflacao_mes_rm:

    #####################################
    #
    # Parameters
    #
    #####################################

    INDICE = Parameter("indice")
    FOLDER = Parameter("folder")
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")

    wait_crawler = crawler(indice=INDICE, folder=FOLDER)
    # pylint: disable=E1123
    filepath = clean_mes_rm(indice=INDICE, upstream_tasks=[wait_crawler])

    create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_type="overwrite",
        wait=filepath,
    )

flow_ibge_inflacao_mes_rm.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_ibge_inflacao_mes_rm.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)


with Flow("Template: IBGE Inflação: mes_municipio") as flow_ibge_inflacao_mes_municipio:

    #####################################
    #
    # Parameters
    #
    #####################################

    INDICE = Parameter("indice")
    FOLDER = Parameter("folder")
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")

    wait_crawler = crawler(indice=INDICE, folder=FOLDER)
    # pylint: disable=E1123
    filepath = clean_mes_municipio(indice=INDICE, upstream_tasks=[wait_crawler])

    create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_type="overwrite",
        wait=filepath,
    )


flow_ibge_inflacao_mes_municipio.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_ibge_inflacao_mes_municipio.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)


with Flow("Template: IBGE Inflação: mes_geral") as flow_ibge_inflacao_mes_geral:

    #####################################
    #
    # Parameters
    #
    #####################################

    INDICE = Parameter("indice")
    FOLDER = Parameter("folder")
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")

    wait_crawler = crawler(indice=INDICE, folder=FOLDER)
    # pylint: disable=E1123
    filepath = clean_mes_geral(indice=INDICE, upstream_tasks=[wait_crawler])

    create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_type="overwrite",
        wait=filepath,
    )

flow_ibge_inflacao_mes_geral.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_ibge_inflacao_mes_geral.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
