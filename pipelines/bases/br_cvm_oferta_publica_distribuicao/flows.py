"""
Flows for br_cvm_oferta_publica_distribuicao
"""

from prefect import Flow
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.bases.br_cvm_oferta_publica_distribuicao.tasks import (
    crawl,
    clean_table_oferta_distribuicao,
)
from pipelines.utils import upload_to_gcs, create_bd_table, create_header
from pipelines.bases.br_cvm_oferta_publica_distribuicao.schedules import every_day

ROOT = "/tmp/basedosdados"
URL = "http://dados.cvm.gov.br/dados/OFERTA/DISTRIB/DADOS/oferta_distribuicao.csv"

with Flow("br_cvm_oferta_publica_distribuicao.dia") as br_cvm_ofe_pub_dis_dia:
    crawl(ROOT, URL)
    filepath = clean_table_oferta_distribuicao(ROOT)
    dataset_id = "br_cvm_oferta_publica_distribuicao"
    table_id =  "dia"

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

br_cvm_ofe_pub_dis_dia.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cvm_ofe_pub_dis_dia.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_cvm_ofe_pub_dis_dia.schedule = every_day
