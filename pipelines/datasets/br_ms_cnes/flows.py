# -*- coding: utf-8 -*-
"""
Flows for br_ms_cnes
"""
# pylint: disable=C0103, E1123, invalid-name

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_ms_cnes.schedules import (
    schedule_br_ms_cnes_dados_complementares,
    schedule_br_ms_cnes_equipamento,
    schedule_br_ms_cnes_equipe,
    schedule_br_ms_cnes_estabelecimento,
    schedule_br_ms_cnes_estabelecimento_ensino,
    schedule_br_ms_cnes_estabelecimento_filantropico,
    schedule_br_ms_cnes_gestao_metas,
    schedule_br_ms_cnes_habilitacao,
    schedule_br_ms_cnes_incentivos,
    schedule_br_ms_cnes_leito,
    schedule_br_ms_cnes_profissional,
    schedule_br_ms_cnes_servico_especializado,
)
from pipelines.utils.crawler_datasus.flows import flow_datasus

br_ms_cnes_profissional = deepcopy(flow_datasus)
br_ms_cnes_profissional.name = "br_ms_cnes.profissional"
br_ms_cnes_profissional.code_owners = ["equipe_pipelines"]
br_ms_cnes_profissional.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_cnes_profissional.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# br_ms_cnes_profissional.schedule = schedule_br_ms_cnes_profissional

br_ms_cnes_estabelecimento = deepcopy(flow_datasus)
br_ms_cnes_estabelecimento.name = "br_ms_cnes.estabelecimento"
br_ms_cnes_estabelecimento.code_owners = ["equipe_pipelines"]
br_ms_cnes_estabelecimento.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_cnes_estabelecimento.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# br_ms_cnes_estabelecimento.schedule = schedule_br_ms_cnes_estabelecimento

br_ms_cnes_leito = deepcopy(flow_datasus)
br_ms_cnes_leito.name = "br_ms_cnes.leito"
br_ms_cnes_leito.code_owners = ["equipe_pipelines"]
br_ms_cnes_leito.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_cnes_leito.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# br_ms_cnes_leito.schedule = schedule_br_ms_cnes_leito

br_ms_cnes_equipamento = deepcopy(flow_datasus)
br_ms_cnes_equipamento.name = "br_ms_cnes.equipamento"
br_ms_cnes_equipamento.code_owners = ["equipe_pipelines"]
br_ms_cnes_equipamento.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_cnes_equipamento.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# br_ms_cnes_equipamento.schedule = schedule_br_ms_cnes_equipamento

br_ms_cnes_equipe = deepcopy(flow_datasus)
br_ms_cnes_equipe.name = "br_ms_cnes.equipe"
br_ms_cnes_equipe.code_owners = ["equipe_pipelines"]
br_ms_cnes_equipe.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_cnes_equipe.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# br_ms_cnes_equipe.schedule = schedule_br_ms_cnes_equipe


br_ms_cnes_dados_complementares = deepcopy(flow_datasus)
br_ms_cnes_dados_complementares.name = "br_ms_cnes.dados_complementares"
br_ms_cnes_dados_complementares.code_owners = ["equipe_pipelines"]
br_ms_cnes_dados_complementares.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_cnes_dados_complementares.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# br_ms_cnes_dados_complementares.schedule = schedule_br_ms_cnes_dados_complementares


br_ms_cnes_servico_especializado = deepcopy(flow_datasus)
br_ms_cnes_servico_especializado.name = "br_ms_cnes.servico_especializado"
br_ms_cnes_servico_especializado.code_owners = ["equipe_pipelines"]
br_ms_cnes_servico_especializado.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_cnes_servico_especializado.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# br_ms_cnes_servico_especializado.schedule = schedule_br_ms_cnes_servico_especializado

br_ms_cnes_habilitacao = deepcopy(flow_datasus)
br_ms_cnes_habilitacao.name = "br_ms_cnes.habilitacao"
br_ms_cnes_habilitacao.code_owners = ["equipe_pipelines"]
br_ms_cnes_habilitacao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_cnes_habilitacao.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# br_ms_cnes_habilitacao.schedule = schedule_br_ms_cnes_habilitacao

br_ms_cnes_gestao_metas = deepcopy(flow_datasus)
br_ms_cnes_gestao_metas.name = "br_ms_cnes.gestao_metas"
br_ms_cnes_gestao_metas.code_owners = ["equipe_pipelines"]
br_ms_cnes_gestao_metas.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_cnes_gestao_metas.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# br_ms_cnes_gestao_metas.schedule = schedule_br_ms_cnes_gestao_metas

br_ms_cnes_incentivos = deepcopy(flow_datasus)
br_ms_cnes_incentivos.name = "br_ms_cnes.incentivos"
br_ms_cnes_incentivos.code_owners = ["equipe_pipelines"]
br_ms_cnes_incentivos.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_cnes_incentivos.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# br_ms_cnes_incentivos.schedule = schedule_br_ms_cnes_incentivos

br_ms_cnes_estabelecimento_ensino = deepcopy(flow_datasus)
br_ms_cnes_estabelecimento_ensino.name = "br_ms_cnes.estabelecimento_ensino"
br_ms_cnes_estabelecimento_ensino.code_owners = ["equipe_pipelines"]
br_ms_cnes_estabelecimento_ensino.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_cnes_estabelecimento_ensino.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# br_ms_cnes_estabelecimento_ensino.schedule = schedule_br_ms_cnes_estabelecimento_ensino

br_ms_cnes_estabelecimento_filantropico = deepcopy(flow_datasus)
br_ms_cnes_estabelecimento_filantropico.name = "br_ms_cnes.estabelecimento_filantropico"
br_ms_cnes_estabelecimento_filantropico.code_owners = ["equipe_pipelines"]
br_ms_cnes_estabelecimento_filantropico.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_cnes_estabelecimento_filantropico.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# br_ms_cnes_estabelecimento_filantropico.schedule = schedule_br_ms_cnes_estabelecimento_filantropico
