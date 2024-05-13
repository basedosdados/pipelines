# -*- coding: utf-8 -*-

# register flow in prefect

from copy import deepcopy
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.utils.crawler_camara_dados_abertos.flows import flow_camara_dados_abertos
from pipelines.datasets.br_camara_dados_abertos.schedules import (
    schedules_br_camara_dados_abertos_votacao,
    schedules_br_camara_dados_abertos_votacao_objeto,
    schedules_br_camara_dados_abertos_votacao_orientacao_bancada,
    schedules_br_camara_dados_abertos_votacao_parlamentar,
    schedules_br_camara_dados_abertos_votacao_proposicao,
    schedules_br_camara_dados_abertos_deputado,
    schedules_br_camara_dados_abertos_deputado_ocupacao,
    schedules_br_camara_dados_abertos_deputado_profissao,
    schedules_br_camara_dados_abertos_proposicao_microdados,
    schedules_br_camara_dados_abertos_proposicao_autor,
    schedules_br_camara_dados_abertos_proposicao_tema,
    schedules_br_camara_dados_abertos_orgao,
    schedules_br_camara_dados_abertos_orgao_deputado,
    schedules_br_camara_dados_abertos_evento,
    schedules_br_camara_dados_abertos_evento_orgao,
    schedules_br_camara_dados_abertos_evento_presenca_deputado,
    schedules_br_camara_dados_abertos_evento_requerimento,
    schedules_br_camara_dados_abertos_funcionario,
    schedules_br_camara_dados_abertos_frente,
    schedules_br_camara_dados_abertos_frente_deputado,
    schedules_br_camara_dados_abertos_licitacao,
    schedules_br_camara_dados_abertos_licitacao_proposta,
    schedules_br_camara_dados_abertos_licitacao_contrato,
    schedules_br_camara_dados_abertos_licitacao_item,
    schedules_br_camara_dados_abertos_licitacao_pedido,
    schedules_br_camara_dados_abertos_despesa)

# ! - > Flow: br_camara_dados_abertos__votacao
br_camara_dados_abertos__votacao = deepcopy(flow_camara_dados_abertos)
br_camara_dados_abertos__votacao.name = "br_camara_dados_abertos.votacao"
br_camara_dados_abertos__votacao.code_owners = ["trick"]
br_camara_dados_abertos__votacao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_camara_dados_abertos__votacao.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_camara_dados_abertos__votacao.schedule = schedules_br_camara_dados_abertos_votacao

# ! - > Flow: br_camara_dados_abertos__votacao_objeto

br_camara_dados_abertos__votacao_objeto = deepcopy(flow_camara_dados_abertos)
br_camara_dados_abertos__votacao_objeto.name = "br_camara_dados_abertos.votacao_objeto"
br_camara_dados_abertos__votacao_objeto.code_owners = ["trick"]
br_camara_dados_abertos__votacao_objeto.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_camara_dados_abertos__votacao_objeto.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_camara_dados_abertos__votacao_objeto.schedule = schedules_br_camara_dados_abertos_votacao_objeto

# ! - > Flow: br_camara_dados_abertos__votacao_orientacao_bancada


br_camara_dados_abertos__votacao_orientacao_bancada = deepcopy(flow_camara_dados_abertos)
br_camara_dados_abertos__votacao_orientacao_bancada.name = "br_camara_dados_abertos.votacao_orientacao_bancada"
br_camara_dados_abertos__votacao_orientacao_bancada.code_owners = ["trick"]
br_camara_dados_abertos__votacao_orientacao_bancada.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_camara_dados_abertos__votacao_orientacao_bancada.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_camara_dados_abertos__votacao_orientacao_bancada.schedule = schedules_br_camara_dados_abertos_votacao_orientacao_bancada

# ! - > Flow: br_camara_dados_abertos__votacao_parlamentar

br_camara_dados_abertos__votacao_parlamentar = deepcopy(flow_camara_dados_abertos)
br_camara_dados_abertos__votacao_parlamentar.name = "br_camara_dados_abertos.votacao_parlamentar"
br_camara_dados_abertos__votacao_parlamentar.code_owners = ["trick"]
br_camara_dados_abertos__votacao_parlamentar.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_camara_dados_abertos__votacao_parlamentar.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_camara_dados_abertos__votacao_parlamentar.schedule = schedules_br_camara_dados_abertos_votacao_parlamentar

# ! - > Flow: br_camara_dados_abertos__votacao_proposicao

br_camara_dados_abertos__votacao_proposicao = deepcopy(flow_camara_dados_abertos)
br_camara_dados_abertos__votacao_proposicao.name = "br_camara_dados_abertos.votacao_proposicao"
br_camara_dados_abertos__votacao_proposicao.code_owners = ["trick"]
br_camara_dados_abertos__votacao_proposicao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_camara_dados_abertos__votacao_proposicao.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_camara_dados_abertos__votacao_proposicao.schedule = schedules_br_camara_dados_abertos_votacao_proposicao

# ! - > Flow: br_camara_dados_abertos__deputado

br_camara_dados_abertos__deputado = deepcopy(flow_camara_dados_abertos)
br_camara_dados_abertos__deputado.name = "br_camara_dados_abertos.deputado"
br_camara_dados_abertos__deputado.code_owners = ["trick"]
br_camara_dados_abertos__deputado.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_camara_dados_abertos__deputado.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_camara_dados_abertos__deputado.schedule = schedules_br_camara_dados_abertos_deputado

# ! - > Flow: br_camara_dados_abertos__deputado_ocupacao

br_camara_dados_abertos__deputado_ocupacao = deepcopy(flow_camara_dados_abertos)
br_camara_dados_abertos__deputado_ocupacao.name = "br_camara_dados_abertos.deputado_ocupacao"
br_camara_dados_abertos__deputado_ocupacao.code_owners = ["trick"]
br_camara_dados_abertos__deputado_ocupacao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_camara_dados_abertos__deputado_ocupacao.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_camara_dados_abertos__deputado_ocupacao.schedule = schedules_br_camara_dados_abertos_deputado_ocupacao

# ! - > Flow: br_camara_dados_abertos__deputado_profissao

br_camara_dados_abertos__deputado_profissao = deepcopy(flow_camara_dados_abertos)
br_camara_dados_abertos__deputado_profissao.name = "br_camara_dados_abertos.deputado_profissao"
br_camara_dados_abertos__deputado_profissao.code_owners = ["trick"]
br_camara_dados_abertos__deputado_profissao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_camara_dados_abertos__deputado_profissao.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_camara_dados_abertos__deputado_profissao.schedule = schedules_br_camara_dados_abertos_deputado_profissao

# ! - > Flow: br_camara_dados_abertos__proposicao_microdados

br_camara_dados_abertos__proposicao_microdados = deepcopy(flow_camara_dados_abertos)
br_camara_dados_abertos__proposicao_microdados.name = "br_camara_dados_abertos.proposicao_microdados"
br_camara_dados_abertos__proposicao_microdados.code_owners = ["trick"]
br_camara_dados_abertos__proposicao_microdados.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_camara_dados_abertos__proposicao_microdados.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_camara_dados_abertos__proposicao_microdados.schedule = schedules_br_camara_dados_abertos_proposicao_microdados

# ! - > Flow: br_camara_dados_abertos__proposicao_autor

br_camara_dados_abertos__proposicao_autor = deepcopy(flow_camara_dados_abertos)
br_camara_dados_abertos__proposicao_autor.name = "br_camara_dados_abertos.proposicao_autor"
br_camara_dados_abertos__proposicao_autor.code_owners = ["trick"]
br_camara_dados_abertos__proposicao_autor.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_camara_dados_abertos__proposicao_autor.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_camara_dados_abertos__proposicao_autor.schedule = schedules_br_camara_dados_abertos_proposicao_autor

# ! - > Flow: br_camara_dados_abertos__proposicao_tema

br_camara_dados_abertos__proposicao_tema = deepcopy(flow_camara_dados_abertos)
br_camara_dados_abertos__proposicao_tema.name = "br_camara_dados_abertos.proposicao_tema"
br_camara_dados_abertos__proposicao_tema.code_owners = ["trick"]
br_camara_dados_abertos__proposicao_tema.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_camara_dados_abertos__proposicao_tema.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_camara_dados_abertos__proposicao_tema.schedule = schedules_br_camara_dados_abertos_proposicao_tema

# ! - > Flow: br_camara_dados_abertos__orgao

br_camara_dados_abertos__orgao = deepcopy(flow_camara_dados_abertos)
br_camara_dados_abertos__orgao.name = "br_camara_dados_abertos.orgao"
br_camara_dados_abertos__orgao.code_owners = ["trick"]
br_camara_dados_abertos__orgao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_camara_dados_abertos__orgao.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_camara_dados_abertos__orgao.schedule = schedules_br_camara_dados_abertos_orgao

# ! - > Flow: br_camara_dados_abertos__orgao_deputado

br_camara_dados_abertos__orgao_deputado = deepcopy(flow_camara_dados_abertos)
br_camara_dados_abertos__orgao_deputado.name = "br_camara_dados_abertos.orgao_deputado"
br_camara_dados_abertos__orgao_deputado.code_owners = ["trick"]
br_camara_dados_abertos__orgao_deputado.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_camara_dados_abertos__orgao_deputado.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_camara_dados_abertos__orgao_deputado.schedule = schedules_br_camara_dados_abertos_orgao_deputado

# ! - > Flow: br_camara_dados_abertos__evento

br_camara_dados_abertos__evento = deepcopy(flow_camara_dados_abertos)
br_camara_dados_abertos__evento.name = "br_camara_dados_abertos.evento"
br_camara_dados_abertos__evento.code_owners = ["trick"]
br_camara_dados_abertos__evento.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_camara_dados_abertos__evento.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_camara_dados_abertos__evento.schedule = schedules_br_camara_dados_abertos_evento

# ! - > Flow: br_camara_dados_abertos__evento_orgao

br_camara_dados_abertos__evento_orgao = deepcopy(flow_camara_dados_abertos)
br_camara_dados_abertos__evento_orgao.name = "br_camara_dados_abertos.evento_orgao"
br_camara_dados_abertos__evento_orgao.code_owners = ["trick"]
br_camara_dados_abertos__evento_orgao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_camara_dados_abertos__evento_orgao.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_camara_dados_abertos__evento_orgao.schedule = schedules_br_camara_dados_abertos_evento_orgao

# ! - > Flow: br_camara_dados_abertos__evento_presenca_deputado

br_camara_dados_abertos__evento_presenca_deputado = deepcopy(flow_camara_dados_abertos)
br_camara_dados_abertos__evento_presenca_deputado.name = "br_camara_dados_abertos.evento_presenca_deputado"
br_camara_dados_abertos__evento_presenca_deputado.code_owners = ["trick"]
br_camara_dados_abertos__evento_presenca_deputado.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_camara_dados_abertos__evento_presenca_deputado.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_camara_dados_abertos__evento_presenca_deputado.schedule = schedules_br_camara_dados_abertos_evento_presenca_deputado

# ! - > Flow: br_camara_dados_abertos__evento_requerimento

br_camara_dados_abertos__evento_requerimento = deepcopy(flow_camara_dados_abertos)
br_camara_dados_abertos__evento_requerimento.name = "br_camara_dados_abertos.evento_requerimento"
br_camara_dados_abertos__evento_requerimento.code_owners = ["trick"]
br_camara_dados_abertos__evento_requerimento.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_camara_dados_abertos__evento_requerimento.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_camara_dados_abertos__evento_requerimento.schedule = schedules_br_camara_dados_abertos_evento_requerimento

# ! - > Flow: br_camara_dados_abertos__funcionario

br_camara_dados_abertos__funcionario = deepcopy(flow_camara_dados_abertos)
br_camara_dados_abertos__funcionario.name = "br_camara_dados_abertos.funcionario"
br_camara_dados_abertos__funcionario.code_owners = ["trick"]
br_camara_dados_abertos__funcionario.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_camara_dados_abertos__funcionario.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_camara_dados_abertos__funcionario.schedule = schedules_br_camara_dados_abertos_funcionario

# ! - > Flow: br_camara_dados_abertos__frente

br_camara_dados_abertos__frente = deepcopy(flow_camara_dados_abertos)
br_camara_dados_abertos__frente.name = "br_camara_dados_abertos.frente"
br_camara_dados_abertos__frente.code_owners = ["trick"]
br_camara_dados_abertos__frente.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_camara_dados_abertos__frente.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_camara_dados_abertos__frente.schedule = schedules_br_camara_dados_abertos_frente

# ! - > Flow: br_camara_dados_abertos__frente_deputado

br_camara_dados_abertos__frente_deputado = deepcopy(flow_camara_dados_abertos)
br_camara_dados_abertos__frente_deputado.name = "br_camara_dados_abertos.frente_deputado"
br_camara_dados_abertos__frente_deputado.code_owners = ["trick"]
br_camara_dados_abertos__frente_deputado.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_camara_dados_abertos__frente_deputado.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_camara_dados_abertos__frente_deputado.schedule = schedules_br_camara_dados_abertos_frente_deputado

# ! - > Flow: br_camara_dados_abertos__licitacao

br_camara_dados_abertos__licitacao = deepcopy(flow_camara_dados_abertos)
br_camara_dados_abertos__licitacao.name = "br_camara_dados_abertos.licitacao"
br_camara_dados_abertos__licitacao.code_owners = ["trick"]
br_camara_dados_abertos__licitacao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_camara_dados_abertos__licitacao.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_camara_dados_abertos__licitacao.schedule = schedules_br_camara_dados_abertos_licitacao

# ! - > Flow: br_camara_dados_abertos__licitacao_contrato

br_camara_dados_abertos__licitacao_contrato = deepcopy(flow_camara_dados_abertos)
br_camara_dados_abertos__licitacao_contrato.name = "br_camara_dados_abertos.licitacao_contrato"
br_camara_dados_abertos__licitacao_contrato.code_owners = ["trick"]
br_camara_dados_abertos__licitacao_contrato.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_camara_dados_abertos__licitacao_contrato.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_camara_dados_abertos__licitacao_contrato.schedule = schedules_br_camara_dados_abertos_licitacao_contrato

# ! - > Flow: br_camara_dados_abertos__licitacao_item

br_camara_dados_abertos__licitacao_item = deepcopy(flow_camara_dados_abertos)
br_camara_dados_abertos__licitacao_item.name = "br_camara_dados_abertos.licitacao_item"
br_camara_dados_abertos__licitacao_item.code_owners = ["trick"]
br_camara_dados_abertos__licitacao_item.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_camara_dados_abertos__licitacao_item.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_camara_dados_abertos__licitacao_item.schedule = schedules_br_camara_dados_abertos_licitacao_item

# ! - > Flow: br_camara_dados_abertos__licitacao_proposta

br_camara_dados_abertos__licitacao_proposta = deepcopy(flow_camara_dados_abertos)
br_camara_dados_abertos__licitacao_proposta.name = "br_camara_dados_abertos.licitacao_proposta"
br_camara_dados_abertos__licitacao_proposta.code_owners = ["trick"]
br_camara_dados_abertos__licitacao_proposta.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_camara_dados_abertos__licitacao_proposta.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_camara_dados_abertos__licitacao_proposta.schedule = schedules_br_camara_dados_abertos_licitacao_proposta

# ! - > Flow: br_camara_dados_abertos__licitacao_pedido

br_camara_dados_abertos__licitacao_pedido = deepcopy(flow_camara_dados_abertos)
br_camara_dados_abertos__licitacao_pedido.name = "br_camara_dados_abertos.licitacao_pedido"
br_camara_dados_abertos__licitacao_pedido.code_owners = ["trick"]
br_camara_dados_abertos__licitacao_pedido.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_camara_dados_abertos__licitacao_pedido.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_camara_dados_abertos__licitacao_pedido.schedule = schedules_br_camara_dados_abertos_licitacao_pedido

# ! - > Flow: br_camara_dados_abertos__despesa

br_camara_dados_abertos__despesa = deepcopy(flow_camara_dados_abertos)
br_camara_dados_abertos__despesa.name = "br_camara_dados_abertos.despesa"
br_camara_dados_abertos__despesa.code_owners = ["trick"]
br_camara_dados_abertos__despesa.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_camara_dados_abertos__despesa.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_camara_dados_abertos__despesa.schedule = schedules_br_camara_dados_abertos_despesa