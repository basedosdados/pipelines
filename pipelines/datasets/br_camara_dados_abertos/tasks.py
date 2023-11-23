# -*- coding: utf-8 -*-
import pandas as pd
from prefect import task

from pipelines.datasets.br_camara_dados_abertos.constants import constants
from pipelines.datasets.br_camara_dados_abertos.utils import (
    get_date,
    read_and_clean_camara_dados_abertos,
)
from pipelines.utils.utils import to_partitions


# ! Microdados
@task
def make_partitions_microdados() -> str:
    df = read_and_clean_camara_dados_abertos(
        path=constants.INPUT_PATH.value,
        table_id="votacoes.csv",
        data="data",
        dict_arquitetura="votacao_microdados",
    )
    print("Particionando microdados")
    to_partitions(
        data=df,
        partition_columns=["ano"],
        savepath=constants.OUTPUT_PATH_MICRODADOS.value,
    )

    return constants.OUTPUT_PATH_MICRODADOS.value


@task
# ! Parlamentar
def make_partitions_parlamentar() -> str:
    df = read_and_clean_camara_dados_abertos(
        path=constants.INPUT_PATH.value,
        table_id="votacoesVotos.csv",
        data="dataHoraVoto",
        dict_arquitetura="voto_parlamentar",
    )
    print("Particionando parlamentar")
    to_partitions(
        data=df,
        partition_columns=["ano"],
        savepath=constants.OUTPUT_PATH_PARLAMENTAR.value,
    )
    return constants.OUTPUT_PATH_PARLAMENTAR.value


@task
# ! Proposição
def make_partitions_proposicao() -> str:
    df = read_and_clean_camara_dados_abertos(
        path=constants.INPUT_PATH.value,
        table_id="votacoesProposicoes.csv",
        data="data",
        dict_arquitetura="votacao_proposicao_afetada",
    )
    print("Particionando proposicao")
    to_partitions(
        data=df,
        partition_columns=["ano"],
        savepath=constants.OUTPUT_PATH_PROPOSICAO.value,
    )

    return constants.OUTPUT_PATH_PROPOSICAO.value


@task
# ! Objeto
def make_partitions_objeto() -> str:
    df = read_and_clean_camara_dados_abertos(
        path=constants.INPUT_PATH.value,
        table_id="votacoesObjetos.csv",
        data="data",
        dict_arquitetura="votacao_objeto",
    )
    print("Particionando objeto")
    to_partitions(
        data=df, partition_columns=["ano"], savepath=constants.OUTPUT_PATH_OBJETO.value
    )

    return constants.OUTPUT_PATH_OBJETO.value


@task
# ! Orientação
def make_partitions_orientacao() -> str:
    df = read_and_clean_camara_dados_abertos(
        path=constants.INPUT_PATH.value,
        table_id="votacoesOrientacoes.csv",
        dict_arquitetura="votacao_orientacao_bancada",
    )
    to_partitions(
        data=df,
        partition_columns=["ano"],
        savepath=constants.OUTPUT_PATH_ORIENTACAO.value,
    )
    return constants.OUTPUT_PATH_ORIENTACAO.value


# ! Obtendo a data máxima.
@task
def get_date_max():
    df = get_date()
    data_max = df["data"].max()

    return data_max
