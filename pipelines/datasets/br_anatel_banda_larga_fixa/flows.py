# -*- coding: utf-8 -*-
"""
Flows for dataset br_anatel_banda_larga_fixa
"""

from copy import deepcopy
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.datasets.br_anatel_banda_larga_fixa.schedules import (
    anatel_microdados,
    anatel_densidade_municipio,
    anatel_densidade_brasil,
    anatel_densidade_uf
)
from pipelines.utils.crawler_anatel.banda_larga_fixa.flows import flow_banda_larga_fixa


# ? Microdados
br_anatel_banda_larga_fixa__microdados = deepcopy(flow_banda_larga_fixa)
br_anatel_banda_larga_fixa__microdados.name = "br_anatel_banda_larga_fixa.microdados"
br_anatel_banda_larga_fixa__microdados.code_owners = ["trick"]
br_anatel_banda_larga_fixa__microdados.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_anatel_banda_larga_fixa__microdados.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_anatel_banda_larga_fixa__microdados.schedule = anatel_microdados

# ? Densidade UF
br_anatel_banda_larga_fixa__densidade_uf = deepcopy(flow_banda_larga_fixa)
br_anatel_banda_larga_fixa__densidade_uf.name = "br_anatel_banda_larga_fixa.densidade_uf"
br_anatel_banda_larga_fixa__densidade_uf.code_owners = ["trick"]
br_anatel_banda_larga_fixa__densidade_uf.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_anatel_banda_larga_fixa__densidade_uf.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_anatel_banda_larga_fixa__densidade_uf.schedule = anatel_densidade_uf

# ? Densidade Brasil
br_anatel_banda_larga_fixa__densidade_brasil = deepcopy(flow_banda_larga_fixa)
br_anatel_banda_larga_fixa__densidade_brasil.name = "br_anatel_banda_larga_fixa.densidade_brasil"
br_anatel_banda_larga_fixa__densidade_brasil.code_owners = ["trick"]
br_anatel_banda_larga_fixa__densidade_brasil.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_anatel_banda_larga_fixa__densidade_brasil.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_anatel_banda_larga_fixa__densidade_brasil.schedule = anatel_densidade_brasil

# ? Densidade Municipio
br_anatel_banda_larga_fixa__densidade_municipio = deepcopy(flow_banda_larga_fixa)
br_anatel_banda_larga_fixa__densidade_municipio.name = "br_anatel_banda_larga_fixa.densidade_municipio"
br_anatel_banda_larga_fixa__densidade_municipio.code_owners = ["trick"]
br_anatel_banda_larga_fixa__densidade_municipio.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_anatel_banda_larga_fixa__densidade_municipio.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_anatel_banda_larga_fixa__densidade_municipio.schedule = anatel_densidade_municipio