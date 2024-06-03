# -*- coding: utf-8 -*-
"""
Flows for br_ms_sim
"""

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from copy import deepcopy
from pipelines.constants import constants
from pipelines.utils.crawler_datasus.flows import flow_simsus

br_ms_sim_microdados = deepcopy(flow_simsus)
br_ms_sim_microdados.name = "br_ms_sim.microdados"
br_ms_sim_microdados.code_owners = ["equipe_pipelines"]
br_ms_sim_microdados.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_sim_microdados.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# br_ms_sim_microdados.schedule = schedule_br_ms_cnes_profissional ## Tabela anual, não vou definir por enquanto.