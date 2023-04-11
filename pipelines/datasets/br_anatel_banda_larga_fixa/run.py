# -*- coding: utf-8 -*-
from pipelines.utils.utils import run_cloud
from pipelines.datasets.br_anatel_banda_larga_fixa.flows import (
    br_anatel,
    br_anatel_densidade_br,
    br_anatel_densidade_uf,
    br_anatel_densidade_municipio,
)

'''run_cloud(
    br_anatel,  # ! O flow que você deseja executar
    labels=[
        "basedosdados-dev",  # ! Label para identificar o agente que irá executar a pipeline (ex: basedosdados-dev)
    ],
    parameters={  # ! True como bool e não como string!
        "materialize_after_dump": False,  # ! Parâmetros que serão passados para a pipeline (opcional)
    },
)


run_cloud(
    br_anatel_densidade_br,  # ! O flow que você deseja executar
    labels=[
        "basedosdados-dev",  # ! Label para identificar o agente que irá executar a pipeline (ex: basedosdados-dev)
    ],
    parameters={  # ! True como bool e não como string!
        "materialize_after_dump": False,  # ! Parâmetros que serão passados para a pipeline (opcional)
    },
)


run_cloud(
    br_anatel_densidade_uf,  # ! O flow que você deseja executar
    labels=[
        "basedosdados-dev",  # ! Label para identificar o agente que irá executar a pipeline (ex: basedosdados-dev)
    ],
    parameters={  # ! True como bool e não como string!
        "materialize_after_dump": False,  # ! Parâmetros que serão passados para a pipeline (opcional)
    },
)'''

run_cloud(
    br_anatel_densidade_municipio,  # ! O flow que você deseja executar
    labels=[
        "basedosdados-dev",  # ! Label para identificar o agente que irá executar a pipeline (ex: basedosdados-dev)
    ],
    parameters={  # ! True como bool e não como string!
        "materialize_after_dump": False,  # ! Parâmetros que serão passados para a pipeline (opcional)
    },
)
