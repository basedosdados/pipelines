# -*- coding: utf-8 -*-
from pipelines.utils.utils import run_cloud
from pipelines.datasets.external_links.flows import (
    external_links_status,
)  # Complete com as infos da sua pipeline

run_cloud(
    external_links_status,  # O flow que você deseja executar
    labels=[
        "basedosdados-dev",  # Label para identificar o agente que irá executar a pipeline (ex: basedosdados-dev)
    ],
    parameters=dict(materialize_after_dump=True),
)
