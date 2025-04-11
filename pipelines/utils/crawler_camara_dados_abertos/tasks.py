# -*- coding: utf-8 -*-
# register tasks
import os
from datetime import timedelta

import requests
from prefect import task

from pipelines.constants import constants
from pipelines.utils.crawler_camara_dados_abertos.constants import (
    constants as constants_camara,
)
from pipelines.utils.crawler_camara_dados_abertos.utils import (
    download_and_read_data,
)
from pipelines.utils.utils import log

# ---------------------------------------> DADOS CAMARA ABERTA - UNIVERSAL


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def save_data(table_id: str) -> str:
    if not os.path.exists(f"{constants_camara.OUTPUT_PATH.value}{table_id}"):
        os.makedirs(f"{constants_camara.OUTPUT_PATH.value}{table_id}")
    df = download_and_read_data(table_id)
    df = [df[0], df[1]]
    output_path = [
        constants_camara.TABLES_OUTPUT_PATH.value[table_id],
        constants_camara.TABLES_OUTPUT_PATH_ANO_ANTERIOR.value[table_id],
    ]
    for output_path_year, df_year in zip(output_path, df):
        if table_id == "proposicao_microdados":
            df_year["ultimoStatus_despacho"] = df_year[
                "ultimoStatus_despacho"
            ].apply(
                lambda x: str(x)
                .replace(";", ",")
                .replace("\n", " ")
                .replace("\r", " ")
            )
            df_year["ementa"] = df_year["ementa"].apply(
                lambda x: str(x)
                .replace(";", ",")
                .replace("\n", " ")
                .replace("\r", " ")
            )
            df_year["ano"] = df_year.apply(
                lambda x: x["dataApresentacao"][0:4]
                if x["ano"] == 0
                else x["ano"],
                axis=1,
            )

        if table_id == "frente_deputado":
            df_year = df_year.rename(
                columns=constants_camara.RENAME_COLUMNS_FRENTE_DEPUTADO.value
            )

        if table_id == "evento":
            df_year = df_year.rename(
                columns=constants_camara.RENAME_COLUMNS_EVENTO.value
            )
            df_year["descricao"] = df_year["descricao"].apply(
                lambda x: str(x).replace("\n", " ").replace("\r", " ")
            )

        if table_id == "votacao":
            df_year["ultimaApresentacaoProposicao_descricao"] = df_year[
                "ultimaApresentacaoProposicao_descricao"
            ].apply(
                lambda x: str(x)
                .replace(";", " ")
                .replace("\n", " ")
                .replace("\r", " ")
            )

        if table_id == "votacao_objeto":
            df_year[["descricao", "proposicao_ementa"]] = df_year[
                ["descricao", "proposicao_ementa"]
            ].apply(
                lambda x: str(x)
                .replace(";", " ")
                .replace("\n", " ")
                .replace("\r", " ")
            )

        if table_id == "votacao_proposicao":
            df_year[["proposicao_ementa"]] = df_year[
                ["proposicao_ementa"]
            ].apply(
                lambda x: str(x)
                .replace(";", " ")
                .replace("\n", " ")
                .replace("\r", " ")
            )

        if table_id == "licitacao_pedido":
            df_year[["observacoes"]] = df_year[["observacoes"]].apply(
                lambda x: str(x)
                .replace(";", " ")
                .replace("\n", " ")
                .replace("\r", " ")
            )

        if table_id == "licitacao_item":
            df_year[["especificacao"]] = df_year[["especificacao"]].apply(
                lambda x: str(x)
                .replace(";", " ")
                .replace("\n", " ")
                .replace("\r", " ")
            )

        log(f"Saving {table_id} to {output_path_year}")

        df_year.to_csv(
            output_path_year, sep=",", index=False, encoding="utf-8"
        )

    return f"{constants_camara.OUTPUT_PATH.value}{table_id}"


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def check_if_url_is_valid(table_id: str) -> bool:
    if (
        requests.get(
            constants_camara.TABLES_URL.value[table_id],
            headers=constants_camara.HEADERS.value,
        ).status_code
        == 200
    ):
        log("URL is valid")
        log(constants_camara.TABLES_URL.value[table_id])
        return True
    elif (
        requests.get(
            constants_camara.TABLES_URL_ANO_ANTERIOR.value[table_id],
            headers=constants_camara.HEADERS.value,
        ).status_code
        == 200
    ):
        log(
            "Table is not available in the current year only in the previous year"
        )
        log(constants_camara.TABLES_URL_ANO_ANTERIOR.value[table_id])
        return False
    else:
        raise ValueError("URL is not valid")
