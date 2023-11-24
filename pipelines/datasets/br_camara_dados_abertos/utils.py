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
        for chave, valor in constants.TABLE_LIST.value.items():
            url_2 = f"http://dadosabertos.camara.leg.br/arquivos/{valor}/csv/{valor}-{ano}.csv"

            response = requests.get(url_2)

            if response.status_code == 200:
                with open(f"{constants.INPUT_PATH.value}{valor}.csv", "wb") as f:
                    f.write(response.content)

            else:
                pass

    log("------------- archive inside in container --------------")
    log(os.listdir(constants.INPUT_PATH.value))


def get_date():
    download_csvs_camara()
    df = pd.read_csv(
        f'{constants.INPUT_PATH.value}{constants.TABLE_LIST.value["votacao_microdados"]}.csv',
        sep=";",
    )

    return df


def read_and_clean_camara_dados_abertos(
    table_id=str, path=constants.INPUT_PATH.value, date_column=str
) -> pd.DataFrame:
    df = pd.read_csv(f"{path}{constants.TABLE_LIST.value[table_id]}.csv", sep=";")

    if table_id == "votacao_orientacao_bancada":
        df["ano"] = constants.ANOS.value[0]

    else:
        df["ano"] = df[date_column].str[0:4]

    log("------------- columns before apply architecture --------------")
    log(f"------------- TABLE ---------------- {table_id} --------------")
    log(df.columns)

    if table_id == "votacao_objeto":
        df.rename(columns=constants.RENAME_COLUMNS_OBJETO.value, inplace=True)
        df = df[constants.ORDER_COLUMNS_OBJETO.value]

    else:
        df = apply_architecture_to_dataframe(
            df,
            url_architecture=constants.TABLE_NAME_ARCHITECTURE.value[table_id],
            apply_include_missing_columns=False,
            apply_column_order_and_selection=True,
            apply_rename_columns=True,
        )
    log("------------- columns after apply architecture --------------")
    log(f"------------- TABLE ---------------- {table_id} ------------")
    log(df.columns)

    return df
