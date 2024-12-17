# -*- coding: utf-8 -*-
from copy import copy, deepcopy
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.utils.crawler_cgu.flows import flow_cgu_receitas_publicas
from pipelines.constants import constants
#from pipelines.datasets.br_cgu_servidores_executivo_federal.schedules import ()
# ! br_cgu_orcamento_publico

br_cgu_orcamento_publico = deepcopy(flow_cgu_receitas_publicas)
br_cgu_orcamento_publico.name = ("br_cgu_orcamento_publico.orcamento")
br_cgu_orcamento_publico.code_owners = ["trick"]
br_cgu_orcamento_publico.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cgu_orcamento_publico.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
#br_cgu_orcamento_publico.schedule = every_day_afastamentos