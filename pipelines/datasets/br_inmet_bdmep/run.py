# -*- coding: utf-8 -*-
from pipelines.utils.utils import run_cloud
from pipelines.datasets.br_inmet_bdmep.flows import (
    br_inmet,
)  # Complete com as infos da sua pipeline

run_cloud(
    br_inmet,  # O flow que você deseja executar
    labels=[
        "basedosdados-dev",  # Label para identificar o agente que irá executar a pipeline (ex: basedosdados-dev)
    ],
    parameters=dict(materialize_after_dump=False),
)
## staging/<branch_id>
