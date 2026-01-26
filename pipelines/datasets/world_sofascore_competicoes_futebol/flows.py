"""
Flows for world_sofascore_competicoes_futebol
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.crawler.world_sofascore_competicoes_futebol.flows import (
    flow_world_sofascore_competicoes_futebol,
)

# Tabela: uefa_champions_league

uefa_champions_league = deepcopy(flow_world_sofascore_competicoes_futebol)
uefa_champions_league.name = (
    "world_sofascore_competicoes_futebol.uefa_champions_league"
)
uefa_champions_league.code_owners = ["luiz"]
uefa_champions_league.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
uefa_champions_league.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# uefa_champions_league.schedule = schedule_uefa_champions_league

# Tabela: brasileirao_serie_a
brasileirao_serie_a = deepcopy(flow_world_sofascore_competicoes_futebol)
brasileirao_serie_a.name = (
    "world_sofascore_competicoes_futebol.brasileirao_serie_a"
)
brasileirao_serie_a.code_owners = ["luiz"]
brasileirao_serie_a.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
brasileirao_serie_a.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# brasileirao_serie_a.schedule = schedule_brasileirao_serie_a
