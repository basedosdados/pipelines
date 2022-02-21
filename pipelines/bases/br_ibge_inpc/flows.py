"""
Flows for br_ibge_inpc
"""

from prefect import Flow
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.utils import upload_to_gcs, create_bd_table, create_header
from pipelines.bases.br_ibge_inpc.tasks import (
    crawler,
    clean_mes_brasil,
    clean_mes_rm,
    clean_mes_municipio,
    clean_mes_geral,
)
from pipelines.bases.br_ibge_inpc.schedules import every_month

INDICE = "inpc"

with Flow("br_ibge_inpc.brasil") as br_ibge_inpc_br:
    FOLDER="br/"
    crawler(INDICE, FOLDER)
    filepath = clean_mes_brasil(INDICE)
    dataset_id = "br_ibge_inpc"
    table_id = "brasil"

    wait_header_path = create_header(path=filepath)

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

br_ibge_inpc_br.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ibge_inpc_br.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_ibge_inpc_br.schedule = every_month

with Flow("br_ibge_inpc.rm") as br_ibge_inpc_rm:
    FOLDER="rm/"
    crawler(INDICE, FOLDER)
    filepath = clean_mes_rm(INDICE)
    dataset_id = "br_ibge_inpc"
    table_id = "rm"

    wait_header_path = create_header(path=filepath)

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

br_ibge_inpc_rm.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ibge_inpc_rm.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_ibge_inpc_rm.schedule = every_month


with Flow("br_ibge_inpc.municipio") as br_ibge_inpc_municipio:
    FOLDER="mun/"
    crawler(INDICE, FOLDER)
    filepath = clean_mes_municipio(INDICE)
    dataset_id = "br_ibge_inpc"
    table_id = "municipio"

    wait_header_path = create_header(path=filepath)

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

br_ibge_inpc_municipio.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ibge_inpc_municipio.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_ibge_inpc_municipio.schedule = every_month

with Flow("br_ibge_inpc.geral") as br_ibge_inpc_geral:
    FOLDER="mes/"
    crawler(INDICE, FOLDER)
    filepath = clean_mes_geral(INDICE)
    dataset_id = "br_ibge_inpc"
    table_id = "geral"

    wait_header_path = create_header(path=filepath)

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

br_ibge_inpc_geral.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ibge_inpc_geral.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_ibge_inpc_geral.schedule = every_month
