"""
Tasks for br_ibge_inpc
"""

import asyncio
import os
import ssl

import numpy as np
import pandas as pd
from prefect import task

from pipelines.crawler.ibge_inflacao.constants import constants
from pipelines.crawler.ibge_inflacao.utils import (
    check_for_update_date,
    check_for_updates,
    collect_data,
    json_categoria,
    json_mes_brasil,
)
from pipelines.utils.utils import to_partitions

# necessary for use wget, see: https://stackoverflow.com/questions/35569042/ssl-certificate-verify-failed-with-python3
ssl._create_default_https_context = ssl._create_unverified_context
# pylint: disable=C0206
# pylint: disable=C0201
# pylint: disable=R0914
# https://sidra.ibge.gov.br/tabela/7062
# https://sidra.ibge.gov.br/tabela/7063
# https://sidra.ibge.gov.br/tabela/7060


@task
def check_for_updates_task(
    table_id: str,
    dataset_id: str,
) -> bool:
    verify = check_for_updates(table_id=table_id, dataset_id=dataset_id)

    return verify[0]


@task
def collect_data_utils(dataset_id: str, table_id: str, periodo=None) -> None:
    if periodo is None:
        periodo = check_for_update_date(
            dataset_id=dataset_id, table_id=table_id
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


@task
def json_to_csv(table_id: str, dataset_id: str, periodo: str):
    if periodo is None:
        periodo = check_for_update_date(
            dataset_id=dataset_id, table_id=table_id
        )
    if table_id == "mes_brasil":
        dados_agrupados = json_mes_brasil(
            dataset_id=dataset_id, table_id=table_id, periodo=periodo
        )
    else:
        dados_agrupados = json_categoria(
            dataset_id=dataset_id, table_id=table_id, periodo=periodo
        )

    df = pd.DataFrame(list(dados_agrupados.values()))
    df = df.apply(lambda x: x.replace("-", np.nan))

    output_path = os.path.join(constants.OUTPUT.value, dataset_id, table_id)
    to_partitions(
        data=df,
        savepath=output_path,
        partition_columns=["ano", "mes"],
    )

    return output_path
