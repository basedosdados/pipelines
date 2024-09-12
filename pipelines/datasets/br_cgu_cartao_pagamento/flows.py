# -*- coding: utf-8 -*-
from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.utils.crawler_cgu.flows import flow_cgu_cartao_pagamento
from pipelines.constants import constants
# from pipelines.datasets.br_ms_sinan.schedules import (
#     everyday_sinan_microdados
# )

br_cgu_cartao_pagamento_governo_federal = deepcopy(flow_cgu_cartao_pagamento)
br_cgu_cartao_pagamento_governo_federal.name = "br_cgu_cartao_pagamento.governo_federal"
br_cgu_cartao_pagamento_governo_federal.code_owners = ["trick"]
br_cgu_cartao_pagamento_governo_federal.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cgu_cartao_pagamento_governo_federal.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
#br_cgu_cartao_pagamento_governo_federal.schedule = everyday_sinan_microdados