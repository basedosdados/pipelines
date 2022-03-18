"""
Flows for br_cvm_administradores_carteira
"""
# pylint: disable=C0103, E1123, invalid-name

from prefect import Flow
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.datasets.br_cvm_administradores_carteira.tasks import (
    crawl,
    clean_table_responsavel,
    clean_table_pessoa_fisica,
    clean_table_pessoa_juridica,
)
from pipelines.constants import constants
from pipelines.utils.tasks import upload_to_gcs, create_bd_table, dump_header_to_csv
from pipelines.datasets.br_cvm_administradores_carteira.schedules import every_day

ROOT = "/tmp/data"
URL = "http://dados.cvm.gov.br/dados/ADM_CART/CAD/DADOS/cad_adm_cart.zip"

with Flow("br_cvm_administradores_carteira.responsavel") as br_cvm_adm_car_res:
    wait_crawl = crawl(root=ROOT, url=URL)
    filepath = clean_table_responsavel(root=ROOT, upstream_tasks=[wait_crawl])
    dataset_id = "br_cvm_administradores_carteira"
    table_id = "responsavel"

    wait_header_path = dump_header_to_csv(data_path=filepath, wait=filepath)

    # Create table in BigQuery
    wait_create_bd_table = create_bd_table(
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

br_cvm_adm_car_res.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cvm_adm_car_res.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_cvm_adm_car_res.schedule = every_day

with Flow("br_cvm_administradores_carteira.pessoa_fisica") as br_cvm_adm_car_pes_fis:
    wait_crawl = crawl(root=ROOT, url=URL)
    filepath = clean_table_pessoa_fisica(root=ROOT, upstream_tasks=[wait_crawl])
    dataset_id = "br_cvm_administradores_carteira"
    table_id = "pessoa_fisica"

    wait_header_path = dump_header_to_csv(data_path=filepath, wait=filepath)

    # Create table in BigQuery
    wait_create_bd_table = create_bd_table(
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

br_cvm_adm_car_pes_fis.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cvm_adm_car_pes_fis.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_cvm_adm_car_pes_fis.schedule = every_day

with Flow("br_cvm_administradores_carteira.pessoa_juridica") as br_cvm_adm_car_pes_jur:
    wait_crawl = crawl(root=ROOT, url=URL)
    filepath = clean_table_pessoa_juridica(root=ROOT, upstream_tasks=[wait_crawl])
    dataset_id = "br_cvm_administradores_carteira"
    table_id = "pessoa_juridica"

    wait_header_path = dump_header_to_csv(data_path=filepath, wait=filepath)

    # Create table in BigQuery
    wait_create_bd_table = create_bd_table(
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

br_cvm_adm_car_pes_jur.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cvm_adm_car_pes_jur.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_cvm_adm_car_pes_jur.schedule = every_day
