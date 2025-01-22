# -*- coding: utf-8 -*-
"""
Flows for br_cgu_licitacao_contrato
"""

from copy import deepcopy, copy
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.utils.crawler_cgu.flows import flow_cgu_licitacao_contrato
from pipelines.constants import constants
from pipelines.datasets.br_cgu_licitacao_contrato.schedules import (
    every_day_contrato_compra,
    every_day_contrato_item,
    every_day_contrato_termo_aditivo,
    every_day_licitacao,
    every_day_licitacao_empenho,
    every_day_licitacao_item,
    every_day_licitacao_participante
)

# ! ------------------ Contrato Compra --------------------

br_cgu_licitacao_contrato__contrato_compra = deepcopy(flow_cgu_licitacao_contrato)
br_cgu_licitacao_contrato__contrato_compra.name = ("br_cgu_licitacao_contrato.contrato_compra")
br_cgu_licitacao_contrato__contrato_compra.code_owners = ["trick"]
br_cgu_licitacao_contrato__contrato_compra.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cgu_licitacao_contrato__contrato_compra.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_cgu_licitacao_contrato__contrato_compra.schedule = every_day_contrato_compra


# ! ------------------ Contrato Item --------------------

br_cgu_licitacao_contrato__contrato_item = deepcopy(flow_cgu_licitacao_contrato)
br_cgu_licitacao_contrato__contrato_item.name = "br_cgu_licitacao_contrato.contrato_item"
br_cgu_licitacao_contrato__contrato_item.code_owners = ["trick"]
br_cgu_licitacao_contrato__contrato_item.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cgu_licitacao_contrato__contrato_item.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_cgu_licitacao_contrato__contrato_item.schedule = every_day_contrato_item

# ! ------------------ Contrato Termo Aditivo ------------------

br_cgu_licitacao_contrato__contrato_termo_aditivo = deepcopy(flow_cgu_licitacao_contrato)
br_cgu_licitacao_contrato__contrato_termo_aditivo.name = ("br_cgu_licitacao_contrato.contrato_termo_aditivo")
br_cgu_licitacao_contrato__contrato_termo_aditivo.code_owners = ["trick"]
br_cgu_licitacao_contrato__contrato_termo_aditivo.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cgu_licitacao_contrato__contrato_termo_aditivo.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_cgu_licitacao_contrato__contrato_termo_aditivo.schedule = (every_day_contrato_termo_aditivo)


# ! ------------------ Licitação ------------------

br_cgu_licitacao_contrato__licitacao = deepcopy(flow_cgu_licitacao_contrato)
br_cgu_licitacao_contrato__licitacao.name = ("br_cgu_licitacao_contrato.licitacao")
br_cgu_licitacao_contrato__licitacao.code_owners = ["trick"]
br_cgu_licitacao_contrato__licitacao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cgu_licitacao_contrato__licitacao.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_cgu_licitacao_contrato__licitacao.schedule = (every_day_licitacao)


# ! ------------------ Licitação Empenho ------------------

br_cgu_licitacao_contrato__licitacao_empenho = deepcopy(flow_cgu_licitacao_contrato)
br_cgu_licitacao_contrato__licitacao_empenho.name = ("br_cgu_licitacao_contrato.licitacao_empenho")
br_cgu_licitacao_contrato__licitacao_empenho.code_owners = ["trick"]
br_cgu_licitacao_contrato__licitacao_empenho.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cgu_licitacao_contrato__licitacao_empenho.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# br_cgu_licitacao_contrato__licitacao_empenho.schedule = (every_day_licitacao_empenho)

# ! ------------------ Licitação Item ------------------

br_cgu_licitacao_contrato__licitacao_item = deepcopy(flow_cgu_licitacao_contrato)
br_cgu_licitacao_contrato__licitacao_item.name = ("br_cgu_licitacao_contrato.licitacao_item")
br_cgu_licitacao_contrato__licitacao_item.code_owners = ["trick"]
br_cgu_licitacao_contrato__licitacao_item.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cgu_licitacao_contrato__licitacao_item.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_cgu_licitacao_contrato__licitacao_item.schedule = (every_day_licitacao_item)


# ! ------------------ Licitação Participante ------------------

br_cgu_licitacao_contrato__licitacao_participante = deepcopy(flow_cgu_licitacao_contrato)
br_cgu_licitacao_contrato__licitacao_participante.name = ("br_cgu_licitacao_contrato.licitacao_participante")
br_cgu_licitacao_contrato__licitacao_participante.code_owners = ["trick"]
br_cgu_licitacao_contrato__licitacao_participante.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cgu_licitacao_contrato__licitacao_participante.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_cgu_licitacao_contrato__licitacao_participante.schedule = (every_day_licitacao_participante)