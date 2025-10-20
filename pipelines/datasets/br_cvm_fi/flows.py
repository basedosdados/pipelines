"""
Flows for br_cvm_fi

"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.crawler.cvm.flows import flow_cvm
from pipelines.datasets.br_cvm_fi.schedules import (
    every_day_balancete,
    every_day_carteiras,
    every_day_extratos,
    every_day_informacao_cadastral,
    every_day_informe,
    every_day_perfil,
)

# Informe Diário
br_cvm_fi__documentos_informe_diario = deepcopy(flow_cvm)
br_cvm_fi__documentos_informe_diario.name = (
    "br_cvm_fi.documentos_informe_diario"
)
br_cvm_fi__documentos_informe_diario.code_owners = ["Luiza"]
br_cvm_fi__documentos_informe_diario.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_cvm_fi__documentos_informe_diario.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_cvm_fi__documentos_informe_diario.schedule = every_day_informe

# Carteiras Fundos de Investimento (CDA)
br_cvm_fi__documentos_carteiras_fundos_investimento = deepcopy(flow_cvm)
br_cvm_fi__documentos_carteiras_fundos_investimento.name = (
    "br_cvm_fi.documentos_carteiras_fundos_investimento"
)
br_cvm_fi__documentos_carteiras_fundos_investimento.code_owners = ["Luiza"]
br_cvm_fi__documentos_carteiras_fundos_investimento.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_cvm_fi__documentos_carteiras_fundos_investimento.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_cvm_fi__documentos_carteiras_fundos_investimento.schedule = (
    every_day_carteiras
)

# Extratos
br_cvm_fi__documentos_extratos_informacoes = deepcopy(flow_cvm)
br_cvm_fi__documentos_extratos_informacoes.name = (
    "br_cvm_fi.documentos_extratos_informacoes"
)
br_cvm_fi__documentos_extratos_informacoes.code_owners = ["Luiza"]
br_cvm_fi__documentos_extratos_informacoes.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_cvm_fi__documentos_extratos_informacoes.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_cvm_fi__documentos_extratos_informacoes.schedule = every_day_extratos

# Perfil Mensal
br_cvm_fi__documentos_perfil_mensal = deepcopy(flow_cvm)
br_cvm_fi__documentos_perfil_mensal.name = "br_cvm_fi.documentos_perfil_mensal"
br_cvm_fi__documentos_perfil_mensal.code_owners = ["Luiza"]
br_cvm_fi__documentos_perfil_mensal.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_cvm_fi__documentos_perfil_mensal.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_cvm_fi__documentos_perfil_mensal.schedule = every_day_perfil

# Informação Cadastral
br_cvm_fi__documentos_informacao_cadastral = deepcopy(flow_cvm)
br_cvm_fi__documentos_informacao_cadastral.name = (
    "br_cvm_fi.documentos_informacao_cadastral"
)
br_cvm_fi__documentos_informacao_cadastral.code_owners = ["Luiza"]
br_cvm_fi__documentos_informacao_cadastral.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_cvm_fi__documentos_informacao_cadastral.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_cvm_fi__documentos_informacao_cadastral.schedule = (
    every_day_informacao_cadastral
)


# Informação Cadastral
br_cvm_fi__documentos_balancete = deepcopy(flow_cvm)
br_cvm_fi__documentos_balancete.name = "br_cvm_fi.documentos_balancete"
br_cvm_fi__documentos_balancete.code_owners = ["Luiza"]
br_cvm_fi__documentos_balancete.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cvm_fi__documentos_balancete.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_cvm_fi__documentos_balancete.schedule = every_day_balancete
