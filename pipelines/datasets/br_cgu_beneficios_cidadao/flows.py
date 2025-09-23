# -*- coding: utf-8 -*-
from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.crawler.cgu.flows import flow_cgu_beneficios_cidadao
from pipelines.datasets.br_cgu_beneficios_cidadao.schedules import (
    every_day_bolsa_familia,
    every_day_bpc,
    every_day_garantia_safra,
)

# ! - > Flow: br_cgu_beneficios_cidadao__novo_bolsa_familia
br_cgu_beneficios_cidadao_bolsa_familia = deepcopy(flow_cgu_beneficios_cidadao)
br_cgu_beneficios_cidadao_bolsa_familia.name = (
    "br_cgu_beneficios_cidadao.novo_bolsa_familia"
)
br_cgu_beneficios_cidadao_bolsa_familia.code_owners = ["trick"]
br_cgu_beneficios_cidadao_bolsa_familia.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_cgu_beneficios_cidadao_bolsa_familia.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_cgu_beneficios_cidadao_bolsa_familia.schedule = every_day_bolsa_familia


# ! - > Flow: br_cgu_beneficios_cidadao__garantia_safra
br_cgu_beneficios_cidadao__garantia_safra = deepcopy(
    flow_cgu_beneficios_cidadao
)
br_cgu_beneficios_cidadao__garantia_safra.name = (
    "br_cgu_beneficios_cidadao.garantia_safra"
)
br_cgu_beneficios_cidadao__garantia_safra.code_owners = ["trick"]
br_cgu_beneficios_cidadao__garantia_safra.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_cgu_beneficios_cidadao__garantia_safra.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_cgu_beneficios_cidadao__garantia_safra.schedule = every_day_garantia_safra

# ! - > br_cgu_beneficios_cidadao__bpc
br_cgu_beneficios_cidadao__bpc = deepcopy(flow_cgu_beneficios_cidadao)
br_cgu_beneficios_cidadao__bpc.name = "br_cgu_beneficios_cidadao.bpc"
br_cgu_beneficios_cidadao__bpc.code_owners = ["trick"]
br_cgu_beneficios_cidadao__bpc.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cgu_beneficios_cidadao__bpc.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_cgu_beneficios_cidadao__bpc.schedule = every_day_bpc
