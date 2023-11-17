# -*- coding: utf-8 -*-
import pandas as pd
from prefect import task

from pipelines.datasets.br_camara_dados_abertos.utils import (
    read_and_clean_microdados,
    read_and_clean_objeto,
    read_and_clean_orientacao,
    read_and_clean_parlamentar,
    read_and_clean_proposicao,
)
from pipelines.utils.utils import to_partitions


# ! Microdados
@task
def make_partitions_microdados() -> str:
    df = read_and_clean_microdados()
    print("Particionando microdados")
    to_partitions(
        data=df, partition_columns=["ano"], savepath="/tmp/output/microdados/"
    )

    return "/tmp/output/microdados/"


@task
# ! Parlamentar
def make_partitions_parlamentar() -> str:
    df = read_and_clean_parlamentar()
    print("Particionando parlamentar")
    to_partitions(
        data=df, partition_columns=["ano"], savepath="/tmp/output/parlamentar/"
    )
    return "/tmp/output/parlamentar/"


@task
# ! Proposição
def make_partitions_proposicao() -> str:
    df = read_and_clean_proposicao()
    print("Particionando proposicao")
    to_partitions(
        data=df, partition_columns=["ano"], savepath="/tmp/output/proporsicao/"
    )

    return "/tmp/output/proporsicao/"


@task
# ! Objeto
def make_partitions_objeto() -> str:
    df = read_and_clean_objeto()
    print("Particionando objeto")
    to_partitions(data=df, partition_columns=["ano"], savepath="/tmp/output/objeto/")

    return "/tmp/output/objeto/"


@task
# ! Orientação
def make_partitions_orientacao() -> str:
    print("Particionando orientacao")
    df = read_and_clean_orientacao()
    to_partitions(
        data=df, partition_columns=["ano"], savepath="/tmp/output/orientacao/"
    )

    return "/tmp/output/orientacao/"


# ! Obtendo a data máxima.
@task
def get_date_max():
    df = read_and_clean_microdados()
    data_max = df["data"].max()

    return data_max
