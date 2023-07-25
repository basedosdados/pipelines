# -*- coding: utf-8 -*-
"""
Tasks for br_b3_cotacoes
"""

from prefect import task
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pipelines.datasets.br_b3_cotacoes.constants import (
    constants as br_b3_cotacoes_constants,
)

from pipelines.utils.utils import (
    log,
)
from pipelines.constants import constants

from pipelines.datasets.br_b3_cotacoes.utils import (
    download_and_unzip,
    read_files,
    partition_data,
)


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def tratamento(delta_day: int):
    """
    Retrieve b3 quotes data for a specific date and create the path to download and open the file.

    Args:

    days_to_run (int): The number of days for which I want to retrieve data from b3.

    Returns:

        str: the file path to download and open b3 files.
    """

    day = (datetime.now() - timedelta(days=delta_day)).strftime("%d-%m-%Y")

    day_url = datetime.strptime(day, "%d-%m-%Y").strftime("%Y-%m-%d")

    download_and_unzip(
        br_b3_cotacoes_constants.B3_URL.value.format(day_url),
        br_b3_cotacoes_constants.B3_PATH_INPUT.value,
    )
    log(
        "********************************ABRINDO O ARQUIVO********************************"
    )

    df = read_files(br_b3_cotacoes_constants.B3_PATH_INPUT_TXT.value.format(day))

    rename = {
        "DataReferencia": "data_referencia",
        "CodigoInstrumento": "codigo_instrumento",
        "AcaoAtualizacao": "acao_atualizacao",
        "PrecoNegocio": "preco_negocio",
        "QuantidadeNegociada": "quantidade_negociada",
        "HoraFechamento": "hora_fechamento",
        "CodigoIdentificadorNegocio": "codigo_identificador_negocio",
        "TipoSessaoPregao": "tipo_sessao_pregao",
        "DataNegocio": "data_negocio",
        "CodigoParticipanteComprador": "codigo_participante_comprador",
        "CodigoParticipanteVendedor": "codigo_participante_vendedor",
    }

    ordem = [
        "data_referencia",
        "tipo_sessao_pregao",
        "codigo_instrumento",
        "acao_atualizacao",
        "data_negocio",
        "codigo_identificador_negocio",
        "preco_negocio",
        "quantidade_negociada",
        "hora_fechamento",
        "codigo_participante_comprador",
        "codigo_participante_vendedor",
    ]

    log(
        "********************************INICIANDO O TRATAMENTO DOS DADOS********************************"
    )
    df.rename(columns=rename, inplace=True)
    df = df.replace(np.nan, "")
    df["codigo_participante_vendedor"] = df["codigo_participante_vendedor"].apply(
        lambda x: str(x).replace(".0", "")
    )
    df["codigo_participante_comprador"] = df["codigo_participante_comprador"].apply(
        lambda x: str(x).replace(".0", "")
    )
    df["preco_negocio"] = df["preco_negocio"].apply(lambda x: str(x).replace(",", "."))
    df["data_referencia"] = pd.to_datetime(df["data_referencia"], format="%Y-%m-%d")
    df["data_negocio"] = pd.to_datetime(df["data_negocio"], format="%Y-%m-%d")
    df["preco_negocio"] = df["preco_negocio"].astype(float)
    df["codigo_identificador_negocio"] = df["codigo_identificador_negocio"].astype(str)
    df["hora_fechamento"] = df["hora_fechamento"].astype(str)
    df["hora_fechamento"] = np.where(
        df["hora_fechamento"].str.len() == 8,
        "0" + df["hora_fechamento"],
        df["hora_fechamento"],
    )
    df["hora_fechamento"] = (
        df["hora_fechamento"].str[0:2]
        + ":"
        + df["hora_fechamento"].str[2:4]
        + ":"
        + df["hora_fechamento"].str[4:6]
        + "."
        + df["hora_fechamento"].str[6:]
    )
    df = df[ordem]
    log(
        "********************************FINALIZANDO O TRATAMENTO DOS DADOS********************************"
    )

    log(
        "********************************INICIANDO PARTICIONAMENTO********************************"
    )

    partition_data(
        df,
        column_name="data_referencia",
        output_directory=br_b3_cotacoes_constants.B3_PATH_OUTPUT.value,
    )

    return br_b3_cotacoes_constants.B3_PATH_OUTPUT.value


def get_today_date():
    d = datetime.now() - timedelta(days=1)

    return d.strftime("%Y-%m-%d")