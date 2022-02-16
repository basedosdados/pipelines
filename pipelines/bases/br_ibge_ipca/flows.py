"""
Flows for br_ibge_ipca
"""

from prefect import Flow
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.utils import upload_to_gcs
from pipelines.bases.br_ibge_ipca.tasks import (
    crawl,
    clean_mes_brasil,
    clean_mes_rm,
    clean_mes_municipio,
    clean_mes_geral,
)
from pipelines.bases.br_ibge_ipca.schedules import every_month

INDICE = "ipca"

with Flow("br_ibge_ipca.brasil") as br_ibge_ipca_br:
    FOLDER="br/"
    crawl(INDICE, FOLDER)
    filepath = clean_mes_brasil(INDICE)
    upload_to_gcs("br_ibge_ipca", "brasil", filepath)

br_ibge_ipca_br.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ibge_ipca_br.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_ibge_ipca_br.schedule = every_month

with Flow("br_ibge_ipca.rm") as br_ibge_ipca_rm:
    FOLDER="rm/"
    crawl(INDICE, FOLDER)
    filepath = clean_mes_rm(INDICE)
    upload_to_gcs("br_ibge_ipca", "rm", filepath)

br_ibge_ipca_rm.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ibge_ipca_rm.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_ibge_ipca_rm.schedule = every_month


with Flow("br_ibge_ipca.municipio") as br_ibge_ipca_municipio:
    FOLDER="mun/"
    crawl(INDICE, FOLDER)
    filepath = clean_mes_municipio(INDICE)
    upload_to_gcs("br_ibge_ipca", "municipio", filepath)

br_ibge_ipca_municipio.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ibge_ipca_municipio.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_ibge_ipca_municipio.schedule = every_month

with Flow("br_ibge_ipca.geral") as br_ibge_ipca_geral:
    FOLDER="mes/"
    crawl(INDICE, FOLDER)
    filepath = clean_mes_geral(INDICE)
    upload_to_gcs("br_ibge_ipca", "geral", filepath)

br_ibge_ipca_geral.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ibge_ipca_geral.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_ibge_ipca_geral.schedule = every_month
