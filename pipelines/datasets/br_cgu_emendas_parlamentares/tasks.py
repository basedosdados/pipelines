import os
import zipfile
from datetime import datetime
from io import BytesIO
from urllib.request import urlopen

import basedosdados as bd
import pandas as pd
from prefect import task

from pipelines.datasets.br_cgu_emendas_parlamentares.constants import constants
from pipelines.utils.utils import log


def download_unzip_file():
    if not os.path.exists(constants.INPUT.value):
        os.mkdir(constants.INPUT.value)
    try:
        r = urlopen(constants.URL.value)
        zip = zipfile.ZipFile(BytesIO(r.read()))
        zip.extractall(path=constants.INPUT.value)
    except Exception as e:
        print(e)
        log("Erro ao baixar e descompactar arquivo")


@task
def convert_str_to_float():
    df = pd.read_csv(
        f"{constants.INPUT.value}EmendasParlamentares.csv",
        sep=";",
        encoding="latin1",
    )
    log("Convertendo valores para float")

    for _ in constants.VALUES_FLOAT.value:
        df[_] = df[_].str.replace(",", ".").astype(float)

    output = f"{constants.OUTPUT.value}microdados.csv"

    if not os.path.exists(constants.OUTPUT.value):
        os.mkdir(constants.OUTPUT.value)

    df.to_csv(output, sep=",", encoding="utf-8", index=False)
    log("---------------- Tabela salva -------------------")
    return constants.OUTPUT.value


@task
def get_last_modified_time():
    download_unzip_file()
    emendas = pd.read_csv(
        f"{constants.INPUT.value}EmendasParlamentares.csv",
        sep=";",
        encoding="latin1",
    )

    data = bd.read_sql(
        constants.QUERY.value,
        billing_project_id="basedosdados",
        from_file=True,
    )

    date = data.iloc[0].to_numpy()
    log("Data da última atualização: " + str(date[1]))
    log("Quantidade de linhas na tabela: " + str(date[0]))
    log("Quantidade de linhas no arquivo: " + str(emendas.shape[0]))
    if emendas.shape[0] > date[0]:
        return datetime.today()

    return date[1]
