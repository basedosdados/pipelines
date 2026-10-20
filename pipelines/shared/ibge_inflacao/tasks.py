"""
Tasks compartilhadas pelos índices de inflação do IBGE
(`br_ibge_ipca`, `br_ibge_ipca15`, `br_ibge_inpc`).

Os 3 datasets chamam exatamente as mesmas 3 tasks (`check_for_updates`,
`collect_data_utils`, `json_to_csv`), parametrizadas só por
`dataset_id`/`table_id` — a diferença entre eles é só lookup nas constantes
(`DATASETS`, `DATASETS_MES_BRASIL`, `GEO_LEVELS`), sem nenhuma lógica
condicional por dataset.

## Em relação ao parâmetro `force_run`

Nos flows dos datasets, mantenha `force_run=false` (default). O
`force_run=true` ignora o guard de "há dado novo?" e força a chamada à API
mesmo quando não há período novo pra baixar. Para a API de agregados do IBGE
(SIDRA, `servicodados.ibge.gov.br/api/v3/agregados`), isso é problemático por
dois motivos:

1. **Janela inexistente.** Quando não há mês novo publicado, a janela
   calculada por `json_mes_brasil`/`next_date_update` pode pedir um período
   que ainda não existe na API. O retorno vem sem o bloco `resultados`
   esperado, e o crawler estoura com `IndexError: list index out of range`.
2. **Limite de 100.000 valores por requisição** (ver seção abaixo). Forçar
   runs fora do schedule aumenta o risco de bater esse limite e receber
   `HTTP 500`.

Em produção, sempre deixe o guard decidir se há trabalho. Use
`force_run=true` apenas para debug local com janela controlada.

## A API de agregados do IBGE impõe restrição de tamanho à requisição

Citação direta da
[documentação oficial](https://servicodados.ibge.gov.br/api/docs/agregados?versao=3):

> Para *Variáveis por agregado, períodos pesquisados e identificador da
> variável* e *Variáveis por agregado e identificador da variável*, cada
> requisição permite retornar no máximo **100.000 valores**. Para saber a
> quantidade de valores retornados, use a seguinte fórmula:
>
> `Nº de categorias x Nº de períodos x Nº de localidades <= 100.000`
>
> Se a requisição exceder 100.000 valores, a API retorna **HTTP 500**.

Implicação prática: a tabela `mes_categoria_municipio` chega perto do
limite (`categorias x meses x municípios` cresce rápido). `collect_data`
chunka as requisições por período pra se manter abaixo do teto. Ao alterar
o cálculo da janela ou o conjunto de localidades, recalcule a estimativa
antes de subir. Em caso de `HTTP 500` em produção, a primeira suspeita é
estouro do teto — não retry cego.
"""

import asyncio
import os
from datetime import date

import numpy as np
import pandas as pd
from prefect import task

from pipelines.shared.ibge_inflacao.constants import constants
from pipelines.shared.ibge_inflacao.utils import (
    collect_data,
    get_date_api,
    json_categoria,
    json_mes_brasil,
    next_date_update,
    order_by_columns,
)
from pipelines.utils.utils import to_partitions


@task
def check_for_updates(
    table_id: str,
    dataset_id: str,
) -> date:
    return get_date_api(table_id=table_id, dataset_id=dataset_id)


@task
def collect_data_utils(
    dataset_id: str, table_id: str, periodo: str | None = None
) -> str:
    if periodo is None:
        periodo = next_date_update(
            dataset_id=dataset_id,
            table_id=table_id,
        )

    if table_id == "mes_brasil":
        asyncio.run(
            collect_data(
                dataset_id=dataset_id,
                table_id=table_id,
                aggregates=constants.DATASETS_MES_BRASIL.value[dataset_id][
                    "aggregates"
                ],
                variables=constants.DATASETS_MES_BRASIL.value[dataset_id][
                    "variables"
                ],
                # pyrefly: ignore [bad-argument-type]
                periods=periodo,
                geo_level=constants.GEO_LEVELS_MES_BRASIL.value,
            )
        )
    else:
        asyncio.run(
            collect_data(
                dataset_id=dataset_id,
                table_id=table_id,
                aggregates=constants.DATASETS.value[dataset_id]["aggregates"],
                variables=constants.DATASETS.value[dataset_id]["variables"],
                # pyrefly: ignore [bad-argument-type]
                periods=periodo,
                geo_level=constants.GEO_LEVELS.value[table_id],
            )
        )

    return periodo


@task
def json_to_csv(table_id: str, dataset_id: str) -> str:
    if table_id == "mes_brasil":
        dados_agrupados = json_mes_brasil(
            dataset_id=dataset_id, table_id=table_id
        )
    else:
        dados_agrupados = json_categoria(
            dataset_id=dataset_id, table_id=table_id
        )

    df = pd.DataFrame(list(dados_agrupados.values()))
    df = df.apply(lambda x: x.replace("-", np.nan))

    df = df[order_by_columns(table_id=table_id)]

    output_path = os.path.join(constants.OUTPUT.value, dataset_id, table_id)
    to_partitions(
        data=df,
        savepath=output_path,
        partition_columns=["ano", "mes"],
    )

    return output_path
