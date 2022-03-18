"""
Flows for br_cvm_oferta_publica_distribuicao
"""

from prefect import Flow
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.datasets.br_cvm_oferta_publica_distribuicao.tasks import (
    crawl,
    clean_table_oferta_distribuicao,
)
from pipelines.utils.tasks import (
    upload_to_gcs,
    create_bd_table,
    dump_header_to_csv,
    publish_table,
)
from pipelines.datasets.br_cvm_oferta_publica_distribuicao.schedules import every_day

ROOT = "/tmp/data"
URL = "http://dados.cvm.gov.br/dados/OFERTA/DISTRIB/DADOS/oferta_distribuicao.csv"

with Flow("br_cvm_oferta_publica_distribuicao.dia") as br_cvm_ofe_pub_dis_dia:
    # pylint: disable=C0103, E1123, invalid-name
    wait_crawl = crawl(root=ROOT, url=URL)
    filepath = clean_table_oferta_distribuicao(root=ROOT, upstream_tasks=[wait_crawl])
    dataset_id = "br_cvm_oferta_publica_distribuicao"
    table_id = "dia"

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
    wait_upload_table = upload_to_gcs(
        path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        wait=wait_create_bd_table,
    )

    publish_table(
        path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        if_exists="replace",
        wait=wait_upload_table,
    )

br_cvm_ofe_pub_dis_dia.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cvm_ofe_pub_dis_dia.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_cvm_ofe_pub_dis_dia.schedule = every_day

# pylint: enable=C0103, E1123, invalid-name
