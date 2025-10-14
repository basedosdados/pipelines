import asyncio
import os

import numpy as np
import pandas as pd
from prefect import task

from pipelines.crawler.ibge_inflacao.constants import constants
from pipelines.crawler.ibge_inflacao.utils import (
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
) -> None:
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

    df = df[order_by_columns(df=df, table_id=table_id)]

    output_path = os.path.join(constants.OUTPUT.value, dataset_id, table_id)
    to_partitions(
        data=df,
        savepath=output_path,
        partition_columns=["ano", "mes"],
    )

    return output_path
