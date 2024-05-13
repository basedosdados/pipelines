# -*- coding: utf-8 -*-
"""
Flows for dataset br_anatel_telefonia_movel
"""

from copy import deepcopy
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from prefect.storage import GCS
from pipelines.datasets.br_anatel_telefonia_movel.schedules import (
            schedule_br_anatel_telefonia_movel__microdados,
            schedule_br_anatel_telefonia_movel__municipio,
            schedule_br_anatel_telefonia_movel__brasil,
            schedule_br_anatel_telefonia_movel__uf)

from pipelines.utils.crawler_anatel.telefonia_movel.flows import flow_telefonia_movel

# ? -------------------------------> Microdados
br_anatel_telefonia_movel__microdados = deepcopy(flow_telefonia_movel)
br_anatel_telefonia_movel__microdados.name = "br_anatel_telefonia_movel.microdados"
br_anatel_telefonia_movel__microdados.code_owners = ["trick"]
br_anatel_telefonia_movel__microdados.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_anatel_telefonia_movel__microdados.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_anatel_telefonia_movel__microdados.schedule = schedule_br_anatel_telefonia_movel__microdados

# ? -------------------------------> Densidade Municipio

br_anatel_telefonia_movel__densidade_municipio = deepcopy(flow_telefonia_movel)
br_anatel_telefonia_movel__densidade_municipio.name = "br_anatel_telefonia_movel.densidade_municipio"
br_anatel_telefonia_movel__densidade_municipio.code_owners = ["trick"]
br_anatel_telefonia_movel__densidade_municipio.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_anatel_telefonia_movel__densidade_municipio.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_anatel_telefonia_movel__densidade_municipio.schedule = schedule_br_anatel_telefonia_movel__municipio

# ? -------------------------------> Densidade UF

br_anatel_telefonia_movel__densidade_uf = deepcopy(flow_telefonia_movel)
br_anatel_telefonia_movel__densidade_uf.name = "br_anatel_telefonia_movel.densidade_uf"
br_anatel_telefonia_movel__densidade_uf.code_owners = ["trick"]
br_anatel_telefonia_movel__densidade_uf.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_anatel_telefonia_movel__densidade_uf.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_anatel_telefonia_movel__densidade_uf.schedule = schedule_br_anatel_telefonia_movel__uf

# ? -------------------------------> Densidade Brasil

br_anatel_telefonia_movel__densidade_brasil = deepcopy(flow_telefonia_movel)
br_anatel_telefonia_movel__densidade_brasil.name = "br_anatel_telefonia_movel.densidade_brasil"
br_anatel_telefonia_movel__densidade_brasil.code_owners = ["trick"]
br_anatel_telefonia_movel__densidade_brasil.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_anatel_telefonia_movel__densidade_brasil.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_anatel_telefonia_movel__densidade_brasil.schedule = schedule_br_anatel_telefonia_movel__brasil