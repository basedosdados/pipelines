# -*- coding: utf-8 -*-
import os

import pandas as pd
import requests

from pipelines.datasets.br_camara_dados_abertos.constants import constants
from pipelines.utils.apply_architecture_to_dataframe.utils import (
    apply_architecture_to_dataframe,
)
from pipelines.utils.utils import log


def download_csvs_camara() -> None:
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
                with open(f"{constants.INPUT_PATH.value}{voto}.csv", "wb") as f:
                    f.write(response.content)

            else:
                pass

    log("------------- archive inside in container --------------")
    log(os.listdir(constants.INPUT_PATH.value))


def get_date():
    df = pd.read_csv(constants.INPUT_PATH.value + "votacoes.csv", sep=";")

    return df


def read_and_clean_camara_dados_abertos(
    table_id=str, path=str, data=str, dict_arquitetura=str
) -> pd.DataFrame:
    if table_id == "votacoesOrientacoes.csv":
        df = pd.read_csv(path + table_id, sep=";")
        log("------------- columns before apply architecture --------------")
        log(df.columns)
        df["ano"] = constants.ANOS.value
        df = apply_architecture_to_dataframe(
            df,
            url_architecture=constants.dict_arquitetura.value[
                "votacao_orientacao_bancada"
            ],
            apply_include_missing_columns=False,
            apply_column_order_and_selection=True,
            apply_rename_columns=True,
        )
        log("------------- columns after apply architecture --------------")
        log(df.columns)

    else:
        df = pd.read_csv(path + table_id, sep=";")
        df["ano"] = df[data].str[0:4]
        log("------------- columns before apply architecture --------------")
        log(df.columns)

        df = apply_architecture_to_dataframe(
            df,
            url_architecture=constants.dict_arquitetura.value[dict_arquitetura],
            apply_include_missing_columns=False,
            apply_column_order_and_selection=True,
            apply_rename_columns=True,
        )
        log("------------- columns after apply architecture --------------")
        log(df.columns)

    return df
