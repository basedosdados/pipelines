# -*- coding: utf-8 -*-
from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.utils.crawler_datasus.flows import flow_sinan
from pipelines.constants import constants
from pipelines.datasets.br_ms_sinan.schedules import (
    everyday_sinan_microdados
)

br_ms_sinan__microdados_dengue = deepcopy(flow_sinan)
br_ms_sinan__microdados_dengue.name = "br_ms_sinan.microdados_dengue"
br_ms_sinan__microdados_dengue.code_owners = ["tricktx"]
br_ms_sinan__microdados_dengue.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_sinan__microdados_dengue.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_ms_sinan__microdados_dengue.schedule = everyday_sinan_microdados
