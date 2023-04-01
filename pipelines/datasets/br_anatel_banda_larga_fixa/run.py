# -*- coding: utf-8 -*-
from pipelines.utils.utils import run_cloud
from pipelines.datasets.br_anatel_banda_larga_fixa.flows import banda_larga_microdados

run_cloud(
    banda_larga_microdados,  # O flow que você deseja executar
    labels=[
        "basedosdados-dev",  # Label para identificar o agente que irá executar a pipeline (ex: basedosdados-dev)
    ],
    parameters={
        "materialize_after_dump": "False",  # Parâmetros que serão passados para a pipeline (opcional)
    },
)
