# -*- coding: utf-8 -*-
from copy import copy, deepcopy
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.utils.crawler_cgu.flows import flow_cgu_receitas_publicas
from pipelines.constants import constants
#from pipelines.datasets.br_cgu_servidores_executivo_federal.schedules import ()
# ! br_cgu_receitas_publicas__receita

br_cgu_receitas_publicas__receitas = deepcopy(flow_cgu_receitas_publicas)
br_cgu_receitas_publicas__receitas.name = ("br_cgu_receitas_publicas.receita")
br_cgu_receitas_publicas__receitas.code_owners = ["trick"]
br_cgu_receitas_publicas__receitas.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cgu_receitas_publicas__receitas.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
#br_cgu_receitas_publicas__receitas.schedule = every_day_afastamentos