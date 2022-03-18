"""
Flows for br_ibge_ipca
"""

from prefect import Flow
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.utils.tasks import upload_to_gcs, create_bd_table, dump_header_to_csv
from pipelines.datasets.br_ibge_ipca.tasks import (
    crawler,
    clean_mes_brasil,
    clean_mes_rm,
    clean_mes_municipio,
    clean_mes_geral,
)
from pipelines.datasets.br_ibge_ipca.schedules import every_month

INDICE = "ipca"

with Flow("br_ibge_ipca.mes_categoria_brasil") as br_ibge_ipca_mes_categoria_brasil:
    FOLDER = "br/"
    wait_crawler = crawler(INDICE, FOLDER)
    filepath = clean_mes_brasil(INDICE, upstream_tasks = [wait_crawler])
    dataset_id = "br_ibge_ipca"
    table_id = "mes_categoria_brasil"

    wait_header_path = dump_header_to_csv(data_path=filepath)

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

br_ibge_ipca_mes_categoria_brasil.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ibge_ipca_mes_categoria_brasil.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_ibge_ipca_mes_categoria_brasil.schedule = every_month

with Flow("br_ibge_ipca.mes_categoria_rm") as br_ibge_ipca_mes_categoria_rm:
    FOLDER = "rm/"
    wait_crawler = crawler(INDICE, FOLDER)
    filepath = clean_mes_rm(INDICE, upstream_tasks = [wait_crawler])
    dataset_id = "br_ibge_ipca"
    table_id = "mes_categoria_rm"

    wait_header_path = dump_header_to_csv(data_path=filepath)

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

br_ibge_ipca_mes_categoria_rm.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ibge_ipca_mes_categoria_rm.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_ibge_ipca_mes_categoria_rm.schedule = every_month


with Flow(
    "br_ibge_ipca.mes_categoria_municipio"
) as br_ibge_ipca_mes_categoria_municipio:
    FOLDER = "mun/"
    wait_crawler = crawler(INDICE, FOLDER)
    filepath = clean_mes_municipio(INDICE, upstream_tasks = [wait_crawler])
    dataset_id = "br_ibge_ipca"
    table_id = "mes_categoria_municipio"

    wait_header_path = dump_header_to_csv(data_path=filepath)

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

br_ibge_ipca_mes_categoria_municipio.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ibge_ipca_mes_categoria_municipio.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_ibge_ipca_mes_categoria_municipio.schedule = every_month

with Flow("br_ibge_ipca.mes_brasil") as br_ibge_ipca_mes_brasil:
    FOLDER = "mes/"
    wait_crawler = crawler(INDICE, FOLDER)
    filepath = clean_mes_geral(INDICE, upstream_tasks = [wait_crawler])
    dataset_id = "br_ibge_ipca"
    table_id = "mes_brasil"

    wait_header_path = dump_header_to_csv(data_path=filepath)

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

br_ibge_ipca_mes_brasil.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ibge_ipca_mes_brasil.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_ibge_ipca_mes_brasil.schedule = every_month
