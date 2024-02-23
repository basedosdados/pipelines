# -*- coding: utf-8 -*-
# register tasks
import os
from datetime import timedelta
import pandas as pd
from prefect import task
from pipelines.constants import constants
from pipelines.utils.crawler_camara_dados_abertos.constants import (
    constants as constants_camara,
)
from pipelines.utils.crawler_camara_dados_abertos.utils import (
    download_and_read_data
)

# ----------------------------------------> DADOS CAMARA ABERTA - UNIVERSAL

@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def save_data(table_id: str) -> str:
    if not os.path.exists(f'{constants_camara.OUTPUT_PATH.value}{table_id}'):
        os.makedirs(f'{constants_camara.OUTPUT_PATH.value}{table_id}')

    df = download_and_read_data(table_id)
    output_path = constants_camara.TABLES_OUTPUT_PATH.value[table_id]

    if table_id == "proposicao_microdados":
        output_path = constants_camara.TABLES_OUTPUT_PATH.value[table_id]
        df["ultimoStatus_despacho"] = df["ultimoStatus_despacho"].apply(
            lambda x: str(x).replace(";", ",").replace("\n", " ").replace("\r", " ")
        )
        df["ementa"] = df["ementa"].apply(
            lambda x: str(x).replace(";", ",").replace("\n", " ").replace("\r", " ")
        )
        df["ano"] = df.apply(
            lambda x: x["dataApresentacao"][0:4] if x["ano"] == 0 else x["ano"],
            axis=1,
        )

    if table_id == "frente_deputado":
        output_path = constants_camara.TABLES_OUTPUT_PATH.value[table_id]
        df = df.rename(columns=constants_camara.RENAME_COLUMNS_FRENTE_DEPUTADO.value)

    if table_id == "evento":
        output_path = constants_camara.TABLES_OUTPUT_PATH.value[table_id]
        df = df.rename(columns=constants_camara.RENAME_COLUMNS_EVENTO.value)
        df["descricao"] = df["descricao"].apply(
            lambda x: str(x).replace("\n", " ").replace("\r", " ")
        )
    print(f"Saving {table_id} to {output_path}")
    df.to_csv(output_path, sep=",", index=False, encoding="utf-8")

    return output_path