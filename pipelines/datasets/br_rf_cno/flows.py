"""
Flows for br_rf_cno - register 03/02/2026
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.crawler.rf.flows import flow_rf

# Microdados
br_rf_cno_microdados = deepcopy(flow_rf)
br_rf_cno_microdados.name = "br_rf_cno.microdados"
br_rf_cno_microdados.code_owners = ["Luiza"]
br_rf_cno_microdados.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_rf_cno_microdados.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    memory_limit="1Gi",
    memory_request="512Mi",
    cpu_limit=0.5,
)
# br_rf_cno_microdados.schedule = schedule_br_rf_cno_microdados

# Vínculos
br_rf_cno_vinculos = deepcopy(flow_rf)
br_rf_cno_vinculos.name = "br_rf_cno.vinculos"
br_rf_cno_vinculos.code_owners = ["Luiza"]
br_rf_cno_vinculos.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    memory_limit="1Gi",
    memory_request="512Mi",
    cpu_limit=0.5,
)
# br_rf_cno_vinculos.schedule = schedule_br_rf_cno_vinculos

# Áreas
br_rf_cno_areas = deepcopy(flow_rf)
br_rf_cno_areas.name = "br_rf_cno.areas"
br_rf_cno_areas.code_owners = ["Luiza"]
br_rf_cno_areas.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    memory_limit="1Gi",
    memory_request="512Mi",
    cpu_limit=0.5,
)
# br_rf_cno_areas.schedule = schedule_br_rf_cno_areas

# Cnaes
br_rf_cno_cnaes = deepcopy(flow_rf)
br_rf_cno_cnaes.name = "br_rf_cno.cnaes"
br_rf_cno_cnaes.code_owners = ["Luiza"]
br_rf_cno_cnaes.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    memory_limit="1Gi",
    memory_request="512Mi",
    cpu_limit=0.5,
)
# br_rf_cno_cnaes.schedule = schedule_br_rf_cno_cnaes
