# -*- coding: utf-8 -*-
"""
Flows for br_ms_sia
"""
# pylint: disable=C0103, E1123, invalid-name

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.utils.crawler_datasus.flows import flow_siasus
from pipelines.constants import constants
from pipelines.datasets.br_ms_sia.schedules import (
    schedule_br_ms_sia_producao_ambulatorial,
)

br_ms_sia_producao_ambulatorial = deepcopy(flow_siasus)
br_ms_sia_producao_ambulatorial.name = "br_ms_sia.producao_ambulatorial"
br_ms_sia_producao_ambulatorial.code_owners = ["Gabriel Pisa"]
br_ms_sia_producao_ambulatorial.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_sia_producao_ambulatorial.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_ms_sia_producao_ambulatorial.schedule = schedule_br_ms_sia_producao_ambulatorial
