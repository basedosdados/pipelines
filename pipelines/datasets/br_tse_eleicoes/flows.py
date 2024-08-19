# -*- coding: utf-8 -*-
"""
Flows for br_tse_eleicoes
"""
# pylint: disable=invalid-name,line-too-long
from datetime import timedelta

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.crawler_tse_eleicoes.flows import br_tse_eleicoes
from copy import deepcopy


# Tabela: candidatos

br_tse_eleicoes_candidatos = deepcopy(br_tse_eleicoes)
br_tse_eleicoes_candidatos.name = "br_tse_eleicoes.candidatos"
br_tse_eleicoes_candidatos.code_owners = ["luiz"]
br_tse_eleicoes_candidatos.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_tse_eleicoes_candidatos.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_tse_eleicoes_candidatos.schedule = None

# Tabela: bens_candidato
br_tse_eleicoes_bens_candidato = deepcopy(br_tse_eleicoes)
br_tse_eleicoes_bens_candidato.name = "br_tse_eleicoes.bens_candidato"
br_tse_eleicoes_bens_candidato.code_owners = ["luiz"]
br_tse_eleicoes_bens_candidato.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_tse_eleicoes_bens_candidato.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_tse_eleicoes_bens_candidato.schedule = None
