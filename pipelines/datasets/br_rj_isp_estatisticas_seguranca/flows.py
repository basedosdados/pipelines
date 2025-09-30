"""
Flows for br_rj_isp_estatisticas_seguranca.
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.crawler.isp.flows import (
    flow_isp,
)
from pipelines.datasets.br_rj_isp_estatisticas_seguranca.schedules import (
    every_month_armas_apreendidas_mensal,
    every_month_evolucao_mensal_cisp,
    every_month_evolucao_mensal_municipio,
    every_month_evolucao_mensal_uf,
    every_month_evolucao_policial_morto_servico_mensal,
    every_month_feminicidio_mensal_cisp,
)

evolucao_mensal_cisp = deepcopy(flow_isp)
evolucao_mensal_cisp.name = (
    "br_rj_isp_estatisticas_seguranca.evolucao_mensal_cisp"
)
evolucao_mensal_cisp.code_owners = ["trick"]
evolucao_mensal_cisp.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
evolucao_mensal_cisp.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
evolucao_mensal_cisp.schedule = every_month_evolucao_mensal_cisp

evolucao_mensal_uf = deepcopy(flow_isp)
evolucao_mensal_uf.name = "br_rj_isp_estatisticas_seguranca.evolucao_mensal_uf"
evolucao_mensal_uf.code_owners = ["trick"]
evolucao_mensal_uf.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
evolucao_mensal_uf.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
evolucao_mensal_uf.schedule = every_month_evolucao_mensal_uf

evolucao_mensal_municipio = deepcopy(flow_isp)
evolucao_mensal_municipio.name = (
    "br_rj_isp_estatisticas_seguranca.evolucao_mensal_municipio  "
)
evolucao_mensal_municipio.code_owners = ["trick"]
evolucao_mensal_municipio.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
evolucao_mensal_municipio.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
evolucao_mensal_municipio.schedule = every_month_evolucao_mensal_municipio

armas_apreendidas_mensal = deepcopy(flow_isp)
armas_apreendidas_mensal.name = (
    "br_rj_isp_estatisticas_seguranca.armas_apreendidas_mensal"
)
armas_apreendidas_mensal.code_owners = ["trick"]
armas_apreendidas_mensal.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
armas_apreendidas_mensal.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
armas_apreendidas_mensal.schedule = every_month_armas_apreendidas_mensal

evolucao_policial_morto_servico_mensal = deepcopy(flow_isp)
evolucao_policial_morto_servico_mensal.name = (
    "br_rj_isp_estatisticas_seguranca.evolucao_policial_morto_servico_mensal"
)
evolucao_policial_morto_servico_mensal.code_owners = ["trick"]
evolucao_policial_morto_servico_mensal.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
evolucao_policial_morto_servico_mensal.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
evolucao_policial_morto_servico_mensal.schedule = (
    every_month_evolucao_policial_morto_servico_mensal
)

feminicidio_mensal_cisp = deepcopy(flow_isp)
feminicidio_mensal_cisp.name = (
    "br_rj_isp_estatisticas_seguranca.feminicidio_mensal_cisp"
)
feminicidio_mensal_cisp.code_owners = ["trick"]
feminicidio_mensal_cisp.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
feminicidio_mensal_cisp.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
feminicidio_mensal_cisp.schedule = every_month_feminicidio_mensal_cisp
