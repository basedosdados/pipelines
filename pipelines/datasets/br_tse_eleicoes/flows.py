# -*- coding: utf-8 -*-
"""
Flows for br_tse_eleicoes
"""

# pylint: disable=invalid-name,line-too-long
from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.crawler_tse_eleicoes.flows import br_tse_eleicoes

# Tabela: candidatos

br_tse_eleicoes_candidatos = deepcopy(br_tse_eleicoes)
br_tse_eleicoes_candidatos.name = "br_tse_eleicoes.candidatos"
br_tse_eleicoes_candidatos.code_owners = ["luiz"]
br_tse_eleicoes_candidatos.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_tse_eleicoes_candidatos.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# br_tse_eleicoes_candidatos.schedule = schedule_candidatos

# Tabela: bens_candidato
br_tse_eleicoes_bens_candidato = deepcopy(br_tse_eleicoes)
br_tse_eleicoes_bens_candidato.name = "br_tse_eleicoes.bens_candidato"
br_tse_eleicoes_bens_candidato.code_owners = ["luiz"]
br_tse_eleicoes_bens_candidato.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_tse_eleicoes_bens_candidato.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# br_tse_eleicoes_bens_candidato.schedule = schedule_bens

# Tabela: despesas_candidato
br_tse_eleicoes_despesas_candidato = deepcopy(br_tse_eleicoes)
br_tse_eleicoes_despesas_candidato.name = "br_tse_eleicoes.despesas_candidato"
br_tse_eleicoes_despesas_candidato.code_owners = ["luiz"]
br_tse_eleicoes_despesas_candidato.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_tse_eleicoes_despesas_candidato.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# br_tse_eleicoes_despesas_candidato.schedule = schedule_despesa

# Tabela: receitas_candidato
br_tse_eleicoes_receitas_candidato = deepcopy(br_tse_eleicoes)
br_tse_eleicoes_receitas_candidato.name = "br_tse_eleicoes.receitas_candidato"
br_tse_eleicoes_receitas_candidato.code_owners = ["luiz"]
br_tse_eleicoes_receitas_candidato.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_tse_eleicoes_receitas_candidato.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# br_tse_eleicoes_receitas_candidato.schedule = schedule_receita
