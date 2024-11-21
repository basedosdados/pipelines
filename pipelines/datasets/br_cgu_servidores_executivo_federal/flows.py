# -*- coding: utf-8 -*-
from copy import copy, deepcopy
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.utils.crawler_cgu.flows import flow_cgu_servidores_publicos
from pipelines.constants import constants
from pipelines.datasets.br_cgu_servidores_executivo_federal.schedules import (
    every_day_afastamentos,
    every_day_cadastro_aposentados,
    every_day_cadastro_pensionistas,
    every_day_cadastro_reserva_reforma_militares,
    every_day_cadastro_servidores,
    every_day_remuneracao,
    every_day_observacoes,
)
# ! br_cgu_servidores_federal__afastamentos

br_cgu_servidores_federal__afastamentos = deepcopy(flow_cgu_servidores_publicos)
br_cgu_servidores_federal__afastamentos.name = ("br_cgu_servidores_executivo_federal.afastamentos")
br_cgu_servidores_federal__afastamentos.code_owners = ["trick"]
br_cgu_servidores_federal__afastamentos.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cgu_servidores_federal__afastamentos.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_cgu_servidores_federal__afastamentos.schedule = every_day_afastamentos

# ! br_cgu_servidores_federal__cadastro_aposentados

br_cgu_servidores_federal__cadastro_aposentados = deepcopy(flow_cgu_servidores_publicos)
br_cgu_servidores_federal__cadastro_aposentados.name = ("br_cgu_servidores_executivo_federal.cadastro_aposentados")
br_cgu_servidores_federal__cadastro_aposentados.code_owners = ["trick"]
br_cgu_servidores_federal__cadastro_aposentados.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cgu_servidores_federal__cadastro_aposentados.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_cgu_servidores_federal__cadastro_aposentados.schedule = every_day_cadastro_aposentados

# ! br_cgu_servidores_federal__cadastro_pensionistas

br_cgu_servidores_federal__cadastro_pensionistas = deepcopy(flow_cgu_servidores_publicos)
br_cgu_servidores_federal__cadastro_pensionistas.name = ("br_cgu_servidores_executivo_federal.cadastro_pensionistas")
br_cgu_servidores_federal__cadastro_pensionistas.code_owners = ["trick"]
br_cgu_servidores_federal__cadastro_pensionistas.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cgu_servidores_federal__cadastro_pensionistas.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_cgu_servidores_federal__cadastro_pensionistas.schedule = every_day_cadastro_pensionistas

# ! br_cgu_servidores_federal__cadastro_reserva_reforma_militares

br_cgu_servidores_federal__cadastro_reserva_reforma_militares = deepcopy(flow_cgu_servidores_publicos)
br_cgu_servidores_federal__cadastro_reserva_reforma_militares.name = ("br_cgu_servidores_executivo_federal.cadastro_reserva_reforma_militares")
br_cgu_servidores_federal__cadastro_reserva_reforma_militares.code_owners = ["trick"]
br_cgu_servidores_federal__cadastro_reserva_reforma_militares.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cgu_servidores_federal__cadastro_reserva_reforma_militares.run_config = (KubernetesRun(image=constants.DOCKER_IMAGE.value))
br_cgu_servidores_federal__cadastro_reserva_reforma_militares.schedule = (every_day_cadastro_reserva_reforma_militares)

# ! br_cgu_servidores_federal__cadastro_servidores

br_cgu_servidores_federal__cadastro_servidores = deepcopy(flow_cgu_servidores_publicos)
br_cgu_servidores_federal__cadastro_servidores.name = ("br_cgu_servidores_executivo_federal.cadastro_servidores")
br_cgu_servidores_federal__cadastro_servidores.code_owners = ["trick"]
br_cgu_servidores_federal__cadastro_servidores.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cgu_servidores_federal__cadastro_servidores.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_cgu_servidores_federal__cadastro_servidores.schedule = every_day_cadastro_servidores

# ! br_cgu_servidores_federal__observacoes

br_cgu_servidores_federal__observacoes = deepcopy(flow_cgu_servidores_publicos)
br_cgu_servidores_federal__observacoes.name = ("br_cgu_servidores_executivo_federal.observacoes")
br_cgu_servidores_federal__observacoes.code_owners = ["trick"]
br_cgu_servidores_federal__observacoes.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cgu_servidores_federal__observacoes.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_cgu_servidores_federal__observacoes.schedule = every_day_observacoes

# ! br_cgu_servidores_federal__remuneracao

br_cgu_servidores_federal__remuneracao = deepcopy(flow_cgu_servidores_publicos)
br_cgu_servidores_federal__remuneracao.name = ("br_cgu_servidores_executivo_federal.remuneracao")
br_cgu_servidores_federal__remuneracao.code_owners = ["trick"]
br_cgu_servidores_federal__remuneracao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cgu_servidores_federal__remuneracao.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_cgu_servidores_federal__remuneracao.schedule = every_day_remuneracao
