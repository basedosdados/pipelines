# -*- coding: utf-8 -*-
from copy import deepcopy
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.utils.crawler_cgu.flows import flow_cgu_beneficios_cidadao
# from pipelines.datasets.br_camara_dados_abertos.schedules import (
#     schedules_br_camara_dados_abertos_votacao,
#    )

# ! - > Flow: br_camara_dados_abertos__votacao
br_cgu_beneficios_cidadao__novo_bolsa_familia = deepcopy(flow_cgu_beneficios_cidadao)
br_cgu_beneficios_cidadao__novo_bolsa_familia.name = "br_cgu_beneficios_cidadao.novo_bolsa_familia"
br_cgu_beneficios_cidadao__novo_bolsa_familia.code_owners = ["trick"]
br_cgu_beneficios_cidadao__novo_bolsa_familia.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cgu_beneficios_cidadao__novo_bolsa_familia.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
#br_cgu_beneficios_cidadao__novo_bolsa_familia.schedule = schedules_br_camara_dados_abertos_votacao