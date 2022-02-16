"""
Flows for br_ibge_ipca15
"""

from prefect import Flow
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.utils import upload_to_gcs
from pipelines.bases.br_ibge_ipca15.tasks import (
    crawl,
    clean_mes_brasil,
    clean_mes_rm,
    clean_mes_municipio,
    clean_mes_geral,
)
from pipelines.bases.br_ibge_ipca15.schedules import every_month

INDICE = "ip15"

with Flow("br_ibge_ipca15.brasil") as br_ibge_ipca15_br:
    FOLDER="br/"
    crawl(INDICE, FOLDER)
    filepath = clean_mes_brasil(INDICE)
    upload_to_gcs("br_ibge_ipca15", "brasil", filepath)

br_ibge_ipca15_br.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ibge_ipca15_br.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_ibge_ipca15_br.schedule = every_month

with Flow("br_ibge_ipca15.rm") as br_ibge_ipca15_rm:
    FOLDER="rm/"
    crawl(INDICE, FOLDER)
    filepath = clean_mes_rm(INDICE)
    upload_to_gcs("br_ibge_ipca15", "rm", filepath)

br_ibge_ipca15_rm.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ibge_ipca15_rm.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_ibge_ipca15_rm.schedule = every_month


with Flow("br_ibge_ipca15.municipio") as br_ibge_ipca15_municipio:
    FOLDER="mun/"
    crawl(INDICE, FOLDER)
    filepath = clean_mes_municipio(INDICE)
    upload_to_gcs("br_ibge_ipca15", "municipio", filepath)

br_ibge_ipca15_municipio.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ibge_ipca15_municipio.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_ibge_ipca15_municipio.schedule = every_month

with Flow("br_ibge_ipca15.geral") as br_ibge_ipca15_geral:
    FOLDER="mes/"
    crawl(INDICE, FOLDER)
    filepath = clean_mes_geral(INDICE)
    upload_to_gcs("br_ibge_ipca15", "geral", filepath)

br_ibge_ipca15_geral.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ibge_ipca15_geral.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_ibge_ipca15_geral.schedule = every_month
