# -*- coding: utf-8 -*-
"""
Flows for br_ibge_ipca15
"""
# pylint: disable=C0103, E1123, invalid-name

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_ibge_ipca15.schedules import (
    br_ibge_ipca15_mes_categoria_brasil_every_month,
    br_ibge_ipca15_mes_categoria_rm_every_month,
    br_ibge_ipca15_mes_categoria_municipio_every_month,
    br_ibge_ipca15_mes_brasil_every_month,
)
from pipelines.utils.crawler_ibge_inflacao.flows import (
    flow_ibge_inflacao_mes_brasil,
    flow_ibge_inflacao_mes_rm,
    flow_ibge_inflacao_mes_municipio,
    flow_ibge_inflacao_mes_geral,
)


br_ibge_ipca15_mes_categoria_brasil = deepcopy(flow_ibge_inflacao_mes_brasil)
br_ibge_ipca15_mes_categoria_brasil.name = "br_ibge_ipca15.mes_categoria_brasil"
br_ibge_ipca15_mes_categoria_brasil.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ibge_ipca15_mes_categoria_brasil.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_ibge_ipca15_mes_categoria_brasil.schedule = (
    br_ibge_ipca15_mes_categoria_brasil_every_month
)


br_ibge_ipca15_mes_categoria_rm = deepcopy(flow_ibge_inflacao_mes_rm)
br_ibge_ipca15_mes_categoria_rm.name = "br_ibge_ipca15.mes_categoria_rm"
br_ibge_ipca15_mes_categoria_rm.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ibge_ipca15_mes_categoria_rm.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_ibge_ipca15_mes_categoria_rm.schedule = br_ibge_ipca15_mes_categoria_rm_every_month

br_ibge_ipca15_mes_categoria_municipio = deepcopy(flow_ibge_inflacao_mes_municipio)
br_ibge_ipca15_mes_categoria_municipio.name = "br_ibge_ipca15.mes_categoria_municipio"
br_ibge_ipca15_mes_categoria_municipio.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ibge_ipca15_mes_categoria_municipio.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_ibge_ipca15_mes_categoria_municipio.schedule = (
    br_ibge_ipca15_mes_categoria_municipio_every_month
)

br_ibge_ipca15_mes_brasil = deepcopy(flow_ibge_inflacao_mes_geral)
br_ibge_ipca15_mes_brasil.name = "br_ibge_ipca15.mes_brasil"
br_ibge_ipca15_mes_brasil.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ibge_ipca15_mes_brasil.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_ibge_ipca15_mes_brasil.schedule = br_ibge_ipca15_mes_brasil_every_month
