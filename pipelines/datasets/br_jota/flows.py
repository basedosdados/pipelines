# -*- coding: utf-8 -*-
"""
Flows for br_jota
"""

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

from pipelines.utils.template_br_jota.flows import br_jota_2024
from pipelines.datasets.br_jota.schedules import (
    schedule_eleicao_perfil_candidato,
    schedule_eleicao_prestacao_contas_candidato,
    schedule_contas_candidato_origem,
    schedule_prestacao_contas_partido
)
from copy import deepcopy


# Tabela: eleicao_perfil_candidato_2024

eleicao_perfil_candidato_2024 = deepcopy(br_jota_2024)
eleicao_perfil_candidato_2024.name = "br_jota.eleicao_perfil_candidato_2024"
eleicao_perfil_candidato_2024.code_owners = ["luiz"]
eleicao_perfil_candidato_2024.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
eleicao_perfil_candidato_2024.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
eleicao_perfil_candidato_2024.schedule = schedule_eleicao_perfil_candidato

# Tabela: eleicao_prestacao_contas_candidato_2024

eleicao_perfil_candidato_2024 = deepcopy(br_jota_2024)
eleicao_perfil_candidato_2024.name = "br_jota.eleicao_prestacao_contas_candidato_2024"
eleicao_perfil_candidato_2024.code_owners = ["luiz"]
eleicao_perfil_candidato_2024.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
eleicao_perfil_candidato_2024.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
eleicao_perfil_candidato_2024.schedule = schedule_eleicao_prestacao_contas_candidato

# Tabela: eleicao_prestacao_contas_candidato_origem_2024

eleicao_perfil_candidato_2024 = deepcopy(br_jota_2024)
eleicao_perfil_candidato_2024.name = "br_jota.eleicao_prestacao_contas_candidato_origem_2024"
eleicao_perfil_candidato_2024.code_owners = ["luiz"]
eleicao_perfil_candidato_2024.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
eleicao_perfil_candidato_2024.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
eleicao_perfil_candidato_2024.schedule = schedule_contas_candidato_origem

# Tabela: eleicao_prestacao_contas_partido_2024

eleicao_perfil_candidato_2024 = deepcopy(br_jota_2024)
eleicao_perfil_candidato_2024.name = "br_jota.eleicao_prestacao_contas_partido_2024"
eleicao_perfil_candidato_2024.code_owners = ["luiz"]
eleicao_perfil_candidato_2024.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
eleicao_perfil_candidato_2024.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
eleicao_perfil_candidato_2024.schedule = schedule_prestacao_contas_partido