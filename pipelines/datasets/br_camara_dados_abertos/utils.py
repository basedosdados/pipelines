# -*- coding: utf-8 -*-
import os

import pandas as pd
import requests

from pipelines.datasets.br_camara_dados_abertos.constants import constants
from pipelines.utils.apply_architecture_to_dataframe.utils import (
    apply_architecture_to_dataframe,
)
from pipelines.utils.utils import log


def download_csvs_camara():
    """
    Docs:
    This function does download all csvs from archives of camara dos deputados.
    The csvs saved in conteiners of docker.

    return:
    None
    """
    log("Downloading csvs from camara dos deputados")
    if not os.path.exists(constants.INPUT_PATH.value):
        os.makedirs(constants.INPUT_PATH.value)

    for ano in constants.ANOS.value:
        for voto in constants.VOTOS.value:
            url_2 = f"http://dadosabertos.camara.leg.br/arquivos/{voto}/csv/{voto}-{ano}.csv"

            response = requests.get(url_2)

            if response.status_code == 200:
                with open(f"{voto}.csv", "wb") as f:
                    f.write(response.content)
            else:
                pass


def get_ano_microdados():
    df = pd.read_csv(constants.INPUT_PATH.value + "votacoes.csv", sep=";")
    df["ano"] = df["data"].str[:4]
    ano_max = df["ano"].max()

    return ano_max


# microdados
def read_and_clean_microdados():
    log("------------- download --------------")
    download_csvs_camara()
    log("------------- archive inside in container --------------")
    log(os.listdir(constants.INPUT_PATH.value))
    log("Read csv from ---- microdados ----")
    df = pd.read_csv(constants.INPUT_PATH.value + "votacoes.csv", sep=";")
    df["ano"] = get_ano_microdados()
    df["horario"] = df["dataHoraRegistro"].str[11:19]
    df = apply_architecture_to_dataframe(
        df,
        url_architecture=constants.dict_arquitetura.value["votacao_microdados"],
        apply_include_missing_columns=False,
        apply_column_order_and_selection=True,
        apply_rename_columns=True,
    )

    return df


def read_and_clean_parlamentar():
    df = pd.read_csv(constants.INPUT_PATH.value + "votacoesVotos.csv", sep=";")
    df["ano"] = get_ano_microdados()
    df = apply_architecture_to_dataframe(
        df,
        url_architecture=constants.dict_arquitetura.value["votacao_parlamentar"],
        apply_include_missing_columns=False,
        apply_column_order_and_selection=True,
        apply_rename_columns=True,
    )

    return df


def read_and_clean_objeto():
    df = pd.read_csv(constants.INPUT_PATH.value + "votacoesObjetos.csv", sep=";")
    df["ano"] = get_ano_microdados()
    df = apply_architecture_to_dataframe(
        df,
        url_architecture=constants.dict_arquitetura.value["votacao_objeto"],
        apply_include_missing_columns=False,
        apply_column_order_and_selection=True,
        apply_rename_columns=True,
    )

    return df


def read_and_clean_orientacao():
    df = pd.read_csv(constants.INPUT_PATH.value + "votacoesOrientacoes.csv", sep=";")
    df["ano"] = get_ano_microdados()
    df = apply_architecture_to_dataframe(
        df,
        url_architecture=constants.dict_arquitetura.value["votacao_orientacao_bancada"],
        apply_include_missing_columns=False,
        apply_column_order_and_selection=True,
        apply_rename_columns=True,
    )

    return df


def read_and_clean_proposicao():
    df = pd.read_csv(constants.INPUT_PATH.value + "votacoesProposicoes.csv", sep=";")
    df["ano"] = get_ano_microdados()
    df = apply_architecture_to_dataframe(
        df,
        url_architecture=constants.dict_arquitetura.value["votacao_proposicao_afetada"],
        apply_include_missing_columns=False,
        apply_column_order_and_selection=True,
        apply_rename_columns=True,
    )

    return df
