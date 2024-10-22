# -*- coding: utf-8 -*-
from copy import deepcopy, copy
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.utils.crawler_cgu.flows import flow_cgu_cartao_pagamento
from pipelines.constants import constants
from pipelines.datasets.br_cgu_cartao_pagamento.schedules import (
    every_day_microdados_compras_centralizadas,
    every_day_microdados_defesa_civil,
    every_day_microdados_governo_federal
)

br_cgu_cartao_pagamento__governo_federal = deepcopy(flow_cgu_cartao_pagamento)
br_cgu_cartao_pagamento__governo_federal.name = "br_cgu_cartao_pagamento.governo_federal"
br_cgu_cartao_pagamento__governo_federal.code_owners = ["trick"]
br_cgu_cartao_pagamento__governo_federal.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cgu_cartao_pagamento__governo_federal.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_cgu_cartao_pagamento__governo_federal.schedule = every_day_microdados_governo_federal

br_cgu_cartao_pagamento__defesa_civil = deepcopy(flow_cgu_cartao_pagamento)
br_cgu_cartao_pagamento__defesa_civil.name = "br_cgu_cartao_pagamento.defesa_civil"
br_cgu_cartao_pagamento__defesa_civil.code_owners = ["trick"]
br_cgu_cartao_pagamento__defesa_civil.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cgu_cartao_pagamento__defesa_civil.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_cgu_cartao_pagamento__defesa_civil.schedule = every_day_microdados_defesa_civil

br_cgu_cartao_pagamento__compras_centralizadas = deepcopy(flow_cgu_cartao_pagamento)
br_cgu_cartao_pagamento__compras_centralizadas.name = "br_cgu_cartao_pagamento.compras_centralizadas"
br_cgu_cartao_pagamento__compras_centralizadas.code_owners = ["trick"]
br_cgu_cartao_pagamento__compras_centralizadas.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cgu_cartao_pagamento__compras_centralizadas.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_cgu_cartao_pagamento__compras_centralizadas.schedule = every_day_microdados_compras_centralizadas