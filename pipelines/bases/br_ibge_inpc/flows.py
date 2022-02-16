"""
Flows for br_ibge_inpc
"""

from prefect import Flow
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.utils import upload_to_gcs
from pipelines.bases.br_ibge_inpc.tasks import (
    crawl,
    clean_mes_brasil,
    clean_mes_rm,
    clean_mes_municipio,
    clean_mes_geral,
)
from pipelines.bases.br_ibge_inpc.schedules import every_month

INDICE = "inpc"

with Flow("br_ibge_inpc.brasil") as br_ibge_inpc_br:
    FOLDER="br/"
    crawl(INDICE, FOLDER)
    filepath = clean_mes_brasil(INDICE)
    upload_to_gcs("br_ibge_inpc", "brasil", filepath)

br_ibge_inpc_br.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ibge_inpc_br.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_ibge_inpc_br.schedule = every_month

with Flow("br_ibge_inpc.rm") as br_ibge_inpc_rm:
    FOLDER="rm/"
    crawl(INDICE, FOLDER)
    filepath = clean_mes_rm(INDICE)
    upload_to_gcs("br_ibge_inpc", "rm", filepath)

br_ibge_inpc_rm.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ibge_inpc_rm.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_ibge_inpc_rm.schedule = every_month


with Flow("br_ibge_inpc.municipio") as br_ibge_inpc_municipio:
    FOLDER="mun/"
    crawl(INDICE, FOLDER)
    filepath = clean_mes_municipio(INDICE)
    upload_to_gcs("br_ibge_inpc", "municipio", filepath)

br_ibge_inpc_municipio.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ibge_inpc_municipio.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_ibge_inpc_municipio.schedule = every_month

with Flow("br_ibge_inpc.geral") as br_ibge_inpc_geral:
    FOLDER="mes/"
    crawl(INDICE, FOLDER)
    filepath = clean_mes_geral(INDICE)
    upload_to_gcs("br_ibge_inpc", "geral", filepath)

br_ibge_inpc_geral.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ibge_inpc_geral.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_ibge_inpc_geral.schedule = every_month
