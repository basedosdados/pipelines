# -*- coding: utf-8 -*-
# register tasks
import os
from datetime import timedelta
import pandas as pd
from prefect import task
import requests
from pipelines.constants import constants
from pipelines.utils.crawler_camara_dados_abertos.constants import (
    constants as constants_camara,
)
from pipelines.utils.crawler_camara_dados_abertos.utils import (
    download_and_read_data
)
from pipelines.utils.utils import log

# ----------------------------------------> DADOS CAMARA ABERTA - UNIVERSAL

@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def save_data(table_id: str) -> str:
    df = download_and_read_data(table_id)
    if not os.path.exists(f'{constants_camara.OUTPUT_PATH.value}{table_id}'):
        os.makedirs(f'{constants_camara.OUTPUT_PATH.value}{table_id}')
    log(f'testando : {constants_camara.OUTPUT_PATH.value}{table_id}')
    output_path = constants_camara.TABLES_OUTPUT_PATH.value[table_id]

    if table_id == "proposicao_microdados":
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
        df = df.rename(columns=constants_camara.RENAME_COLUMNS_FRENTE_DEPUTADO.value)

    if table_id == "evento":
        df = df.rename(columns=constants_camara.RENAME_COLUMNS_EVENTO.value)
        df["descricao"] = df["descricao"].apply(
            lambda x: str(x).replace("\n", " ").replace("\r", " ")
        )

    if table_id == "votacao":
        df['ultimaApresentacaoProposicao_descricao'] = df['ultimaApresentacaoProposicao_descricao'].apply(lambda x: str(x).replace(';', ' ').replace("\n", " ").replace("\r", " "))

    if table_id == "votacao_objeto":
        df[['descricao', "proposicao_ementa"]] = df[['descricao', "proposicao_ementa"]].apply(lambda x: str(x).replace(';', ' ').replace("\n", " ").replace("\r", " "))

    if table_id == "votacao_proposicao":
        df[["proposicao_ementa"]] = df[["proposicao_ementa"]].apply(lambda x: str(x).replace(';', ' ').replace("\n", " ").replace("\r", " "))

    if table_id == "licitacao_pedido":
        df[["observacoes"]] = df[["observacoes"]].apply(lambda x: str(x).replace(';', ' ').replace("\n", " ").replace("\r", " "))

    if table_id == "licitacao_item":
        df[["especificacao"]] = df[["especificacao"]].apply(lambda x: str(x).replace(';', ' ').replace("\n", " ").replace("\r", " "))

    log(f"Saving {table_id} to {output_path}")

    df.to_csv(output_path, sep=",", index=False, encoding="utf-8")

    return output_path

@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def check_if_url_is_valid(table_id:str) -> bool:
    if requests.get(constants_camara.TABLES_URL.value[table_id]).status_code == 200:
        log("URL is valid")
        return True
    elif requests.get(constants_camara.TABLES_URL_ANO_ANTERIOR.value[table_id]).status_code == 200:
        log("Table is not available in the current year only in the previous year")
        return False
    else:
        raise ValueError("URL is not valid")
