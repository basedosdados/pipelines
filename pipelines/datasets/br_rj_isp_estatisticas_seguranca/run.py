# -*- coding: utf-8 -*-
from pipelines.utils.utils import run_cloud
from pipelines.datasets.br_rj_isp_estatisticas_seguranca.flows import (
    evolucao_mensal_municipio,
    evolucao_mensal_uf,
    evolucao_mensal_cisp,
    taxa_evolucao_mensal_uf,
    taxa_evolucao_mensal_municipio,
    feminicidio_mensal_cisp,
    evolucao_policial_morto_servico_mensal,
    armas_apreendidas_mensal,
)


run_cloud(
    feminicidio_mensal_cisp,
    labels=[
        "basedosdados-dev",
    ],
    parameters = dict(
        materialize_after_dump = True,
    )
)

run_cloud(
    armas_apreendidas_mensal,
    labels=[
        "basedosdados-dev",
    ],
    parameters=dict(
        materialize_after_dump=True,
    ),
)

run_cloud(
    evolucao_mensal_cisp,
    labels=[
        "basedosdados-dev",
    ],
    parameters = dict(
        materialize_after_dump = True,
    )
)
run_cloud(
    taxa_evolucao_mensal_uf,
    labels=[
        "basedosdados-dev",
    ],
    parameters = dict(
        materialize_after_dump = True,
    )
)

run_cloud(
    taxa_evolucao_mensal_municipio,
    labels=[
        "basedosdados-dev",
    ],
    parameters = dict(
        materialize_after_dump = True,
    )
)

run_cloud(
    evolucao_mensal_municipio,
    labels=[
        "basedosdados-dev",
    ],
    parameters = dict(
        materialize_after_dump = True,
    )
)

run_cloud(
    evolucao_policial_morto_servico_mensal,
    labels=[
        "basedosdados-dev",
    ],
    parameters = dict(
        materialize_after_dump = True,
    )
)
run_cloud(
    armas_apreendidas_mensal,
    labels=[
        "basedosdados-dev",
    ],
    parameters = dict(
        materialize_after_dump = True,
    )
)
run_cloud(
    evolucao_mensal_municipio,
    labels=[
        "basedosdados-dev",
    ],
    parameters = dict(
        materialize_after_dump = True,
    )
)

run_cloud(
    evolucao_mensal_municipio,
    labels=[
        "basedosdados-dev",
    ],
    parameters = dict(
        materialize_after_dump = True,
    )
)

run_cloud(
    evolucao_mensal_uf,
    labels=[
        "basedosdados-dev",
    ],
    parameters = dict(
        materialize_after_dump = True,
    )
)

