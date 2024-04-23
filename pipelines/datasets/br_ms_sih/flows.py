# -*- coding: utf-8 -*-
"""
Flows for br_ms_sih
"""
# pylint: disable=C0103, E1123, invalid-name

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.utils.crawler_datasus.flows import flow_sihsus
from pipelines.constants import constants
# from pipelines.datasets.br_ms_sih.schedules import (

# )

br_ms_sih_servicos_profissionais = deepcopy(flow_sihsus)
br_ms_sih_servicos_profissionais.name = "br_ms_sih.servicos_profissionais"
br_ms_sih_servicos_profissionais.code_owners = ["arthurfg"]
br_ms_sih_servicos_profissionais.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_sih_servicos_profissionais.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
#br_ms_sia_producao_ambulatorial.schedule = schedule_br_ms_sia_producao_ambulatorial
